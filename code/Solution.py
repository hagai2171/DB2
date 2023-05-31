from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    base_tables = """"
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
                free_space integer NOT NULL CHECK (disk_free_space_needed >= 0),
                cost_per_byte integer NOT NULL CHECK (cost_per_byte > 0)
            );
         CREATE TABLE IF NOT EXISTS "Ram"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                size integer NOT NULL CHECK (size > 0),
                company TEXT NOT NULL
            );
    """



def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    return Photo()


def deletePhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


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


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
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


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []
