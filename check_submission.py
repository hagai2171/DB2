import zipfile
import sys

# zip format must be ID1-ID2.zip contains ID1_ID2.txt, ID1_ID2.pdf, Solution.py
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Please enter the zip file from this directory')
        exit(1)
    zip_file = sys.argv[1]
    try:
        ids = zip_file.split('.zip')[0]
        id1, id2 = ids.split("-")[0], ids.split("-")[1]
    except:
        print('Must be ID1-ID2.zip')
        exit(1)
    if len(id1) != 9 or len(id2) != 9:
        print('IDs must be 9 digits')
        exit(1)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        list_Of_files = zip_ref.namelist()
        if len(list_Of_files) != 3:
            print('There must be 3 files in zip')
            exit(1)
        if "Solution.py" not in list_Of_files:
            print('Solution.py is missing')
            exit(1)
        if id1 + "_" + id2 + ".pdf" not in list_Of_files:
            print(id1 + "_" + id2 + ".pdf" + ' is missing')
            exit(1)
        if id1 + "_" + id2 + ".txt" not in list_Of_files:
            print(id1 + "_" + id2 + ".txt" + ' is missing')
            exit(1)
    print('Success, IDs are: ' + str(id1) + ", " + str(id2))
