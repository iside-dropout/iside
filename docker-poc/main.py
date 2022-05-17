import io
import logging
import os
import pickle
import tempfile
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from dataset_load import dataset_to_csv
from dataset_minio import get_from_s3, put_into_s3
from decrypt import get_email_addresses, init_encrypt
from send_email import send_email

# Constants, prepare (load .env, check config, etc)
SENTINELS = ['None', 'n.d.', ' ', '']
DELTA = 180  # 6 months
STUDENTS_IN_STUDY = 20

load_dotenv()
logging.basicConfig(level=logging.INFO)


# Script body
def diff_month(d): return abs((datetime.today() - d)).days


def supp_lab(row):
    """It takes into account all other features (besides the character) necessary for the labeling"""

    res = True
    taken = float(row[22]) if row[22] != 'None' else 0
    booked = float(row[20]) if row[20] != 'None' else 0
    tot = float(row[6]) if row[6] != 'n.d' else 0
    if tot != 0 and (booked + taken) == tot:  # Ha finito il percorso
        res = False
    elif row[21] != 'None' or row[23] != 'None':  # Se ha almeno una delle due date
        d_booked = int(row[21]) if pd.notnull(row[21]) else 946684800
        d_taken = int(row[23]) if row[23] != 'None' else 946684800
        last_date = max(d_booked, d_taken)
        if diff_month(datetime.fromtimestamp(last_date)) > DELTA:
            res = True  # Dropout
        else:
            res = False  # NON dropout
    else:
        res = True

    return res


def lab_fun(row):
    """Function to apply for labeling, it also calls supp_lab"""

    if row[len(row) - 1] != 'None':  # Ha il login
        dll = datetime.fromtimestamp(int(row[len(row) - 1]))
        if diff_month(dll) <= DELTA:
            return False  # NON Dropout
        else:
            return supp_lab(row)
    else:  # Qui entra sia se non ha il login e sia se ce l'ha ma è troppo vecchio
        return supp_lab(row)


def preprocess_data(df):
    """Data pre-processing"""

    # Excluding those kinds of undesirable students (check data types)
    df = df[(df.erasmus == 'NO') & (df.isMedicina == False) & (df.annoCorso != "") &
            (pd.notnull(df.codCorso)) & (df.diplomando != 1) & (pd.notnull(df.annoCorso))
            & (pd.notnull(df.annoAccaCors)) & (pd.notnull(df.annoDiNascita))
            & (pd.notnull(df.primaIscr)) & (pd.notnull(df.ultIscr)) & (pd.notnull(df.cfuTake))
            & (pd.notnull(df.cfuPrenotati)) & (pd.notnull(df.lastLogin))]

    # Labeling
    df = df.assign(Dropout=[False] * df.shape[0])
    df.Dropout = df.apply(lab_fun, axis=1)

    # Dropping rows with strange dates
    df = df[(pd.isnull(df['annoCorso']) == False) & (
        df['primaIscr'] > 2000) & (df['annoDiNascita'] > 1970)]

    v_dates = ["annoAccaCors", "annoDiNascita", "primaIscr", "ultIscr"]
    df = df.astype(
        {'annoCorso': np.int32, "primaIscr": np.int32, "ultIscr": np.int32})
    for el in v_dates:
        df[el] = df[el].apply(lambda x: int(
            round(time.mktime(datetime.strptime(str(x), '%Y').timetuple()))))

    # Working on types
    val = {"SI": True, "NO": False}
    df["iscritto"] = df["iscritto"].map(val)
    val = {"M": True, "F": False}
    df["sesso"] = df["sesso"].map(val)
    val = {"true": True, "false": False}
    df["nonDichiaro"] = df["nonDichiaro"].map(val)
    df = df.astype(
        {'annoCorso': np.int32, 'annoAccaCors': np.int32, 'annoDiNascita': np.int32, 'iscritto': bool, 'sesso': bool,
         'primaIscr': np.int32,
         'ultIscr': np.int32, 'cfuTake': np.int32, 'cfuPrenotati': np.int32, 'lastLogin': np.int64})

    # Should be equal to yid_out.tsv
    df.rename(columns={'voto': 'votoDiploma'}, inplace=True)

    # From now on the final dataset (yid_out.tsv) is preprocessed to be passed to the model
    # Dropping useless features and imputing the ones with missing values
    df = df.drop(['diplomando', 'erasmus', 'isMedicina',
                  'luogoDiNascita', 'nazioneNascita', 'codiTiso'], axis=1)
    df.nonDichiaro.fillna(True, inplace=True)
    df.valoreIntero.fillna(df.valoreIntero.mean(), inplace=True)
    df.mediaVoto.fillna(df.mediaVoto.mean(), inplace=True)
    df.votoDiploma.fillna(round(df.votoDiploma.mean()), inplace=True)
    df = df.astype(
        {'nonDichiaro': bool, 'valoreIntero': np.int64, 'votoDiploma': np.int32})
    df.loc[df.valoreIntero < 0, 'valoreIntero'] = 50000
    index_names = df[df['mediaVoto'] > 31].index
    df.drop(index_names, inplace=True)
    val = {True: 1, False: 0}
    df["iscritto"] = df["iscritto"].map(val)
    df["sesso"] = df["sesso"].map(val)
    df = df.astype(
        {'iscritto': np.int32, 'sesso': np.int32, 'nonDichiaro': np.int32})

    # Highly related features (with "Dropout"), we remove them
    df = df.drop(['Dropout', 'lastLogin',
                  'annoAccaCors', 'ultIscr', 'nonDichiaro', 'creditiTotali', 'ultPren', 'ultSup'], axis=1)

    return df  # Equivalent to yid_out


def load_dataset():
    """Get the latest dataset from S3/Minio/CSV based on env variables"""

    if "S3_HOST" in os.environ:
        # Get ZIP from S3 and decompress it in a temporary directory (we can't load everything in RAM or pipeline)
        logging.info("Get dataset from S3")
        dataset_dir = tempfile.TemporaryDirectory()
        get_from_s3(os.environ["S3_HOST"],
                    os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"],
                    os.environ["S3_BUCKET"],
                    dataset_dir.name,
                    "S3_INSECURE" not in os.environ)

        # # Load the JSON dataset to a TSV in RAM
        logging.info("Create in-memory TSV")
        in_memory_csv = io.StringIO()

        dataset_to_csv(dataset_dir.name, in_memory_csv)

        # Create a pandas DataFrame from the in-memory string
        logging.info("Create pandas DataFrame")
        in_memory_csv.seek(0)
        ds = pd.read_csv(in_memory_csv, sep='\t', na_values=SENTINELS)
    else:
        ds = pd.read_csv("../assets/out.tsv", sep='\t', na_values=SENTINELS)

    # Start preprocessing data
    ds = ds[(ds['codCorso'] == 29923) | (ds['codCorso'] == 29932)]
    ds = preprocess_data(ds)
    mat = ds.ID_Stud

    # Load the OneHotEncoder
    logging.info("Load OneHotEncoder")
    with open('../assets/encoder.pickle', 'rb') as f:
        encoder = pickle.load(f)

    logging.info("OneHotEncoder loaded, transforming data")
    # Applying OHE on the categorical features of ds
    ohe_elements = ['cittadinanza', 'facolta', 'tipoIscrizione']
    feature_arr = encoder.transform(ds[ohe_elements]).toarray()
    feature_labels = encoder.get_feature_names_out()
    feature_labels = np.array(feature_labels).ravel()
    encoded_df = pd.DataFrame(feature_arr, columns=feature_labels)
    ds = ds.reset_index(drop=True)
    ds = ds.join(encoded_df)
    ds = ds.drop(ohe_elements, axis=1)

    # OHE creates these column with no sense
    if "facolta_PSICOLOGIA 1" in ds.columns:
        ds = ds.drop(['ID_Stud', 'facolta_PSICOLOGIA 1'], axis=1)

    return mat, ds


def load_predictor():
    """Load pre-trained things"""

    # Load the network/forest/tree/whatever
    logging.info("Load predictor")
    with open('../assets/gbdt_model.pickle', 'rb') as f:
        predictor = pickle.load(f)

    return predictor


def save_results(res):
    in_memory_csv = io.BytesIO()
    res.to_csv(in_memory_csv)

    in_memory_csv.seek(0, os.SEEK_END)
    datalen = in_memory_csv.tell()

    in_memory_csv.seek(0)
    put_into_s3(os.environ["S3_HOST"],
                os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"],
                os.environ["S3_BUCKET"],
                "results_%s.csv" % (datetime.now(timezone.utc).isoformat()),
                in_memory_csv,
                datalen,
                secure_connection="S3_INSECURE" not in os.environ)


def main():
    logging.info("Starting")

    mat, ds = load_dataset()
    predictor = load_predictor()

    logging.info("Predicting")
    y_prob = predictor.predict_proba(ds)
    # y_sc = y_pred[:, 0]

    # Match matricula with predicted probabilities
    res = pd.DataFrame(zip(mat, y_prob[:, 1]), columns=['matricola', 'Dropout'])
    # Sort the resulting dataframe with the most probable dropouts at the top
    # y_sc[::-1].sort()
    res.sort_values(by='Dropout', inplace=True, ascending=False)

    logging.info("Saving results")
    save_results(res)

    headlist = res.head(STUDENTS_IN_STUDY)
    t20 = headlist[headlist["Dropout"] >= 0.7]
    m20 = headlist[headlist["Dropout"] < 0.7]
    print("head")
    print(t20)
    print("middle")
    print(m20)

    print("tail")
    b20 = res.tail(STUDENTS_IN_STUDY)
    print(b20)

    init_encrypt()

    logging.info("Sending emails")
    emails = get_email_addresses(t20["matricola"].values.tolist())
    for email in emails:
        send_email(email, "")

    # Middle-20
    emails = get_email_addresses(m20["matricola"].values.tolist())
    for email in emails:
        send_email(email, "")

    # Bottom-20
    emails = get_email_addresses(b20["matricola"].values.tolist())
    for email in emails:
        send_email(email, "")


if __name__ == '__main__':
    main()
