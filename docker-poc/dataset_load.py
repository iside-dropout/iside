import csv
import json
import time
from datetime import datetime
from os import listdir

import numpy as np

DATASET_FILES_SUFFIX = ['profile.json', 'isee.json', 'exams.booked.json', 'exams.taken.json',
                        'diplomas.json']  # Lista dei file da aprire per ogni studente

FEATURES_PROFILE = ["annoAccaCors", "annoCorso", "annoDiNascita", "cittadinanza", "codCorso", "creditiTotali",
                    "erasmus", "facolta", "flagIdentificato", "isMedicina", "iscritto",
                    "luogoDiNascita", "nazioneNascita", "primaIscr", "sesso", "tipoCorso", "tipoIscrizione",
                    "tipoStudente", "ultIscr"]
FEATURES_ISEE = ["nonDichiaro", "valoreIntero"]
FEATURES_BOOKED = ["cfuPrenotati", "ultPren"]
# Esito -->"valoreNonNominale"
FEATURES_TAKEN = ["cfuTake", "ultSup", "mediaVoto"]
FEATURES_DIPLOMAS = ["voto", "diplomando", "codiTiso"]
FEATURES_COUNTRIES = ["AUSTRIA", "BELGIO", "REPUBBLICA di BULGARIA", "REPUBBLICA di CIPRO", "CROAZIA", "DANIMARCA",
                      "ESTONIA", "FINLANDIA", "FRANCIA", "GERMANIA",
                      "REPUBBLICA FEDERALE TEDESCA", "GRECIA", "IRLANDA", "LETTONIA", "LITUANIA", "LUSSEMBURGO",
                      "MALTA",
                      "PAES BASSI", "POLONIA", "PORTOGALLO", "REPUBBLICA CECA",
                      "ROMANIA", "SLOVACCHIA", "SLOVENIA", "SPAGNA", "SVEZIA", "UNGHERIA"]


def take_info_booked(data):
    student_info = []
    if data['appelli']:
        tot_cfu = 0
        last_date = []
        for el in data['appelli']:
            tot_cfu += el['crediti']
            last_date.append(datetime.strptime(
                el['dataAppe'], '%d/%m/%Y')) if el['dataAppe'] is not None else None
        student_info.append(tot_cfu)
        last_date.sort()
        student_info.append(int(round(last_date[-1].timestamp()))) if last_date != [
        ] else None
    else:
        return [0, 946684800]  # Timestamp of 2000-01-01
    return student_info


def take_info_profile(data, v):
    student_info = []
    if data:
        for el in v:
            # Cittadinanza a grana media, togliere questo if per tornare all'originale
            if el == "cittadinanza":
                if data['nazioneNascita'] == "ITALIA":
                    student_info.append("ITA")
                elif data['nazioneNascita'] in FEATURES_COUNTRIES:
                    student_info.append("UE")
                else:
                    student_info.append("Extra-UE")
            elif el == "primaIscr" or el == "ultIscr":
                student_info.append(
                    int(data[el].split('/')[0])) if data[el] != '' else student_info.append('None')
            else:
                student_info.append(data[el])
    else:
        for _ in v:
            student_info.append('None')
    return student_info


def take_info_diplomas(data, v):
    student_info = []
    if data:
        for el in v:
            if el == 'voto' and data['lode'] == '1':
                student_info.append(101)
            # Scala il voto dei diplomi esteri
            elif el == 'voto' and data['baseVoto'] > '0' and data['baseVoto'] != '100':
                voto = round(
                    (float(data[el]) * 100) / int(data['baseVoto']))
                student_info.append(voto)
            elif el == 'voto' and data['diplomando'] != '1':
                # Gli indiani pur avendo base 100 hanno voti finali decimali
                student_info.append(round(float(data[el])))
            else:
                student_info.append(data[el])
    else:
        for _ in v:
            student_info.append('None')
    return student_info


def take_info_isee(data, v):
    student_info = []
    if data:
        for el in v:
            student_info.append(data[el])
    else:
        for _ in v:
            student_info.append('None')
    return student_info


def take_info_taken(data, v):
    student_info = []
    if data['esami'] != [] and data['esami'] is not None:
        tot_cfu = 0
        last_date = []
        grades = []
        for el in data['esami']:
            tot_cfu += el['cfu']
            last_date.append(datetime.strptime(
                el['data'], '%d/%m/%Y')) if el['data'] is not None else None
            grades.append(el['esito']['valoreNonNominale']
                          ) if not el['esito'][
                'nominale'] else None  # Se è un'idoneità considera i crediti ma non va a cercare il voto
        student_info.append(tot_cfu)
        last_date.sort()
        student_info.append(int(round(last_date[-1].timestamp()))) if last_date != [
        ] else None  # min prende la piu' recente
        student_info.append(int(np.average(grades))) if grades != [
        ] else student_info.append(None)
    else:
        return [0, 946684800, 0]
    return student_info


def take_info_ll(data):
    if data != '':
        return int(round(time.mktime(datetime.strptime(
            data, '%Y-%m-%dT%H:%M:%SZ').timetuple())))
    else:
        return 946684800


def dataset_to_csv(dataset_dir, csv_out_file):
    """Transform the JSON dataset into CSV

    :param dataset_dir: Directory of the JSON dataset
    :type dataset_dir: string
    :param csv_out_file: CSV output TextIOWrapper (e.g. open() result or io.StringIO)
    :type csv_out_file: TextIOWrapper
    """

    # List all student IDs. Since we have multiple files with the same student ID, we consider only the first of the
    # types of files (DATASET_FILES_SUFFIX)
    students = []
    for fname in listdir(dataset_dir):
        if fname.endswith(DATASET_FILES_SUFFIX[0]):
            students.append(int(fname.split(".")[0]))

    writer = csv.writer(csv_out_file, delimiter='\t')

    # Add CSV heading
    csv_heading = ['ID_Stud']
    for datagroup in [FEATURES_PROFILE, FEATURES_ISEE, FEATURES_BOOKED, FEATURES_TAKEN, FEATURES_DIPLOMAS]:
        csv_heading.extend(datagroup)
    csv_heading.append("lastLogin")
    writer.writerow(csv_heading)

    # Process all students
    for student_id in students:
        student_info = [student_id]

        # For each student: open all dataset files and load interesting keys (FEATURES_*)
        for suffix in DATASET_FILES_SUFFIX:
            file_name = dataset_dir + "/" + str(student_id) + '.' + suffix
            try:
                with open(file_name) as in_data:
                    tmp = json.load(in_data)

                    # Load different infos depending on the file that we're considering
                    if suffix == 'exams.booked.json':
                        student_info.extend(
                            take_info_booked(tmp.pop('ritorno')))
                    elif suffix == 'isee.json':
                        student_info.extend(take_info_isee(tmp, FEATURES_ISEE))
                    elif suffix == 'profile.json':
                        student_info.extend(take_info_profile(
                            tmp.pop('ritorno'), FEATURES_PROFILE))
                    elif suffix == 'diplomas.json':
                        if tmp is not None:
                            student_info.extend(take_info_diplomas(
                                tmp[0], FEATURES_DIPLOMAS))
                        else:
                            student_info.extend(
                                ['None' for _ in FEATURES_DIPLOMAS])
                    elif suffix == 'exams.taken.json':
                        tmp.pop('esito')
                        student_info.extend(take_info_taken(
                            tmp['ritorno'], FEATURES_TAKEN))
            except FileNotFoundError:
                if suffix == 'exams.booked.json':
                    student_info.extend([0, 946684800])
                elif suffix == 'isee.json':
                    student_info.extend(['None' for _ in FEATURES_ISEE])
                elif suffix == 'profile.json':
                    student_info.extend(['None' for _ in FEATURES_PROFILE])
                elif suffix == 'diplomas.json':
                    student_info.extend(['None' for _ in FEATURES_DIPLOMAS])
                elif suffix == 'exams.taken.json':
                    student_info.extend([0, 946684800, 0])
            except Exception as e:
                raise e

        # Load last login info (TXT, not json)
        try:
            with open((dataset_dir + str(student_id) + '.lastlogin.txt')) as in_data:
                student_info.append(take_info_ll(in_data.read()))
        except:
            student_info.append(946684800)

        # Save student row in CSV file
        writer.writerow(student_info)
