from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

# ************************************** our auxiliary functions start **************************************
def create_base_tables():
    return """
        CREATE TABLE IF NOT EXISTS "Photo"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                description TEXT NOT NULL,
                disk_free_space_needed integer NOT NULL CHECK (disk_free_space_needed >= 0)
            );
        CREATE TABLE IF NOT EXISTS "Disk"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                manufacturing_company TEXT NOT NULL,
                speed integer NOT NULL CHECK (speed > 0),
                free_space integer NOT NULL CHECK (free_space >= 0),
                cost_per_byte integer NOT NULL CHECK (cost_per_byte > 0)
            );
         CREATE TABLE IF NOT EXISTS "Ram"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                size integer NOT NULL CHECK (size > 0),
                company TEXT NOT NULL
            );
    """
def create_new_tables():
    return """
            CREATE TABLE IF NOT EXISTS "PhotoInDisk"
		(
			photo_id integer NOT NULL,
			disk_id integer NOT NULL,
			PRIMARY KEY (photo_id, disk_id),
			FOREIGN KEY (photo_id) REFERENCES "Photo" (id) ON DELETE CASCADE
			FOREIGN KEY (disk_id) REFERENCES "Disk" (id) ON DELETE CASCADE,
		);
    """
def create_view_tables():
    return """
    """
# generically add tuple to table
def add(query) -> ReturnValue:
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except (DatabaseException.CHECK_VIOLATION,
            DatabaseException.NOT_NULL_VIOLATION,
            DatabaseException.FOREIGN_KEY_VIOLATION) as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except (DatabaseException.ConnectionInvalid,
            DatabaseException.database_ini_ERROR,
            DatabaseException.UNKNOWN_ERROR,
            Exception) as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result

# generically delete tuple from table
def delete(query):
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            conn.commit()
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


# ************************************** our auxiliary functions end **************************************

# ************************************** Database functions start **************************************
def createTables():
    base_tables = create_base_tables()
    new_tables = create_new_tables()
    views = create_view_tables()
    query = base_tables + new_tables + views
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except Exception as e:
        pass
    finally:
        conn.close()

def clearTables():
    base_tables = ["Photo", "Disk", "Ram"]
    queries = ['DELETE FROM "{}";'.format(table) for table in base_tables]
    query = "\n".join(queries)
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except Exception as e:
        pass
    finally:
        conn.close()


def dropTables():
    base_tables = ["Photo", "Disk", "Ram"]
    queries = ['DROP TABLE IF EXISTS "{}";'.format(table) for table in base_tables]
    query = "\n".join(queries)
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except Exception as e:
        pass
    finally:
        conn.close()

# ************************************** Database functions end **************************************

# ************************************** CRUD API functions start **************************************
# Python's equivalent to NULL is None.
# You can assume the arguments to the function will not be None, the inner attributes of the
# argument might consist of None
def addPhoto(photo: Photo) -> ReturnValue:
    query = sql.SQL('INSERT INTO "Photo" VALUES ({photo_id}, {description}, {disk_free_space_needed})').format(
        photo_id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        disk_free_space_needed=sql.Literal(photo.getSize())
    )
    return add(query)


def getPhotoByID(photoID: int) -> Photo:
    # Photo getPhotoByID(Int photoID)
    # Returns the photo object of photoID.
    # Input: Photo ID.
    # Output: The photo object in case the photo exists. BadPhoto() otherwise
    result = Photo.badPhoto()
    query = sql.SQL('SELECT * FROM "Photo" WHERE id = {id} ').format(id=sql.Literal(photoID))
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected == 1:
            photo_id, description, size = entries[0].values()
            result.setPhotoID(photo_id)
            result.setDescription(description)
            result.setSize(size)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result

def deletePhoto(photo: Photo) -> ReturnValue:
    query = sql.SQL(
        """"
        UPDATE "Disk" SET free_space = free_space + {photo_size} WHERE id IN
            (SELECT PhotoInDisk.Disk_id FROM 
                ((SELECT id FROM "Photo" WHERE id = {id_to_del}) AS Photo_to_del)
                INNER JOIN
                PhotoInDisk ON Photo_to_del.id == PhotoInDisk.photo_id);         
        DELETE FROM "Photo" where id = {id_to_del};
        """).format(
        photo_size = sql.Literal(photo.getSize()),
        id_to_del=sql.Literal(photo.getPhotoID()))
    return delete(query)
# createTables()
# addPhoto(Photo(1, "Tree", 10))
# print(getPhotoByID(1).__str__())
# clearTables()
# dropTables()
createTables()
addPhoto(Photo(1, "Tree", 10))
print(getPhotoByID(1).__str__())
# deletePhoto(Photo(1, "Tree", 10))

def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK

def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:

    return ReturnValue.OK
# ************************************** CRUD API functions end **************************************

# ************************************** BASIC API functions start **************************************

def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    # The photo with photo.ID is now saved on disk with diskID only if the photo's size is
    # not larger than the free space on disk.
    # Input: The photo that needs to be saved on disk with diskID.
    # Output: ReturnValue with the following conditions:
    # * OK in case of success.
    # * NOT_EXISTS if photo/disk does not exist.
    # * ALREADY_EXISTS if the photo is already saved on the disk.
    # * BAD_PARAMS in case the photo's size is larger than the free space on the disk.
    # * ERROR in case of a database error.
    # Note: do not forget to adjust the free space on the disk.
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    return 0


def getTotalRamOnDisk(diskID: int) -> int:
    return 0


def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []

# ************************************** BASIC API functions end **************************************

# ************************************** ADVANCED API functions start **************************************

def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []

# ************************************** ADVANCED API functions end **************************************