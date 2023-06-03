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
         CREATE TABLE IF NOT EXISTS "RAM"
            (
                id integer NOT NULL PRIMARY KEY CHECK (id > 0),
                size integer NOT NULL CHECK (size > 0),
                company TEXT NOT NULL
            );
    """


def create_new_tables():
    return create_photo_in_disk_table() + create_ram_in_disk_table()


def create_photo_in_disk_table():
    return """
        CREATE TABLE IF NOT EXISTS "PhotoInDisk"
            (
                photo_id integer NOT NULL,
                disk_id integer NOT NULL,
                PRIMARY KEY (photo_id, disk_id),
                FOREIGN KEY (photo_id) REFERENCES "Photo" (id) ON DELETE CASCADE,
                FOREIGN KEY (disk_id) REFERENCES "Disk" (id) ON DELETE CASCADE
            );
    """


def create_ram_in_disk_table():
    return """
        CREATE TABLE IF NOT EXISTS "RAMInDisk"
    		(
    			ram_id integer NOT NULL,
    			disk_id integer NOT NULL,
    			PRIMARY KEY (ram_id, disk_id),
    			FOREIGN KEY (ram_id) REFERENCES "RAM" (id) ON DELETE CASCADE,
    			FOREIGN KEY (disk_id) REFERENCES "Disk" (id) ON DELETE CASCADE
    		);
    """


def create_view_tables():
    return """
        CREATE VIEW "TotalRAMInDisk" as 
        select "Disk".id as disk_id,COALESCE(SUM("RAM".size), 0) as total_ram
        from "Disk"
        left outer join "RAMInDisk" on "Disk".id = "RAMInDisk".disk_id
        left outer join  "RAM" on "RAM".id = "RAMInDisk".ram_id
        GROUP BY "Disk".id;
    """


# generically add tuple to table
def add(query) -> ReturnValue:
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        result = ReturnValue.ALREADY_EXISTS
    except Exception:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


# generically delete tuple from table
def delete(query, is_disk=False):
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            conn.commit()
        else:
            if is_disk:
                result = ReturnValue.NOT_EXISTS
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
    base_tables = ["Photo", "Disk", "RAM"]
    new_tables = ["PhotoInDisk", "RAMInDisk"]
    queries = ['DELETE FROM "{table}";'.format(table=table) for table in base_tables + new_tables]
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
    base_tables = ["Photo", "Disk", "RAM"]
    new_tables = ["PhotoInDisk", "RAMInDisk"]
    view_tables = ["TotalRAMInDisk"]
    queries = ['DROP TABLE IF EXISTS "{table}" CASCADE;'.format(table=table) for table in
               base_tables + new_tables + view_tables]
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
        UPDATE "Disk" SET free_space = free_space + {size} WHERE id IN
            (SELECT "PhotoInDisk".Disk_id FROM 
                "PhotoInDisk" INNER JOIN "Photo" ON "Photo".id = "PhotoInDisk".photo_id 
                WHERE ("Photo".id, "Photo".description, "Photo".size) = ({id}, {description}, {size});        
        DELETE FROM "Photo" WHERE (id, description, size) = ({id}, {description}, {size});
        """).format(
        id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        size=sql.Literal(photo.getSize()))
    return delete(query)


def addDisk(disk: Disk) -> ReturnValue:
    query = sql.SQL(
        'INSERT INTO "Disk" VALUES ({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});').format(
        disk_id=sql.Literal(disk.getDiskID()),
        manufacturing_company=sql.Literal(disk.getCompany()),
        speed=sql.Literal(disk.getSpeed()),
        free_space=sql.Literal(disk.getFreeSpace()),
        cost_per_byte=sql.Literal(disk.getCost())
    )
    return add(query)


def getDiskByID(diskID: int) -> Disk:
    result = Disk.badDisk()
    query = sql.SQL('SELECT * FROM "Disk" WHERE id = {id} ').format(id=sql.Literal(diskID))
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected == 1:
            disk_id, manufacturing_company, speed, free_space, cost_per_byte = entries[0].values()
            result.setDiskID(disk_id)
            result.setCompany(manufacturing_company)
            result.setSpeed(speed)
            result.setFreeSpace(free_space)
            result.setCost(cost_per_byte)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result


def deleteDisk(diskID: int) -> ReturnValue:
    query = sql.SQL('DELETE FROM "Disk" where id = {id}').format(id=sql.Literal(diskID))
    return delete(query=query, is_disk=True)


def addRAM(ram: RAM) -> ReturnValue:
    query = sql.SQL('INSERT INTO "RAM" VALUES ({id}, {size}, {company})').format(
        id=sql.Literal(ram.getRamID()),
        size=sql.Literal(ram.getSize()),
        company=sql.Literal(ram.getCompany())
    )
    return add(query)


def getRAMByID(ramID: int) -> RAM:
    result = RAM.badRAM()
    query = sql.SQL('SELECT * FROM "RAM" WHERE id = {id} ').format(id=sql.Literal(ramID))
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected == 1:
            ram_id, size, company = entries[0].values()
            result.setRamID(ram_id)
            result.setCompany(company)
            result.setSize(size)
    except Exception as e:
        pass
    finally:
        conn.close()
        return result


def deleteRAM(ramID: int) -> ReturnValue:
    query = sql.SQL(
        'DELETE FROM "RAM" where id = {id}').format(
        id=sql.Literal(ramID))
    return delete(query=query, is_disk=False)


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    query = sql.SQL("""
        INSERT INTO "Disk" VALUES ({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});
        INSERT INTO "Photo" VALUES ({photo_id}, {description}, {disk_free_space_needed});
        """).format(
        disk_id=sql.Literal(disk.getDiskID()),
        manufacturing_company=sql.Literal(disk.getCompany()),
        speed=sql.Literal(disk.getSpeed()),
        free_space=sql.Literal(disk.getFreeSpace()),
        cost_per_byte=sql.Literal(disk.getCost()),
        photo_id=sql.Literal(photo.getPhotoID()),
        description=sql.Literal(photo.getDescription()),
        disk_free_space_needed=sql.Literal(photo.getSize())
    )
    return add(query)


# ************************************** CRUD API functions end **************************************

# ************************************** BASIC API functions start **************************************

def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    query = sql.SQL("""
        INSERT INTO "PhotoInDisk" values ({photo_id},{disk_id});
        UPDATE "Disk" set free_space = free_space - {photo_size} where id = {disk_id};
    """).format(
        photo_id=sql.Literal(photo.getPhotoID()),
        photo_size=sql.Literal(photo.getSize()),
        disk_id=sql.Literal(diskID))
    return add(query)


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    query = sql.SQL("""
        UPDATE "Disk" set free_space=free_space+(
            select "Photo".disk_free_space_needed
            from "Photo"
            inner join  "PhotoInDisk" on "PhotoInDisk".disk_id = {diskID} and "Photo".id = "PhotoInDisk".photo_id and "Photo".id= {photoID}
            ) where id = {diskID};
        DELETE FROM "PhotoInDisk" where Photo_id = {photoID} and disk_id = {diskID};
        """).format(
        photoID=sql.Literal(photo.getPhotoID()),
        PhotoSize=sql.Literal(photo.getSize()),
        diskID=sql.Literal(diskID))
    return delete(query=query, is_disk=False)


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    query = sql.SQL("""INSERT INTO "RAMInDisk" VALUES ({ram_id},{disk_id} )""").format(
        ram_id=sql.Literal(ramID),
        disk_id=sql.Literal(diskID)
    )
    return add(query)


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    query = sql.SQL("""
        DELETE FROM "RAMInDisk" where ram_id = {ramID} and disk_id = {diskID};
        """).format(
        ramID=sql.Literal(ramID),
        diskID=sql.Literal(diskID))
    return delete(query=query, is_disk=False)


def averagePhotosSizeOnDisk(diskID: int) -> float:
    avg_size = 0
    query = sql.SQL("""
           select AVG("Photo".disk_free_space_needed)
           from "Photo"
           inner join "PhotoInDisk" on "PhotoInDisk".disk_id = {diskID} and "Photo".id = "PhotoInDisk".photo_id
           """).format(
        diskID=sql.Literal(diskID))
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            avg_size = entries.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()
    return avg_size


def getTotalRamOnDisk(diskID: int) -> int:
    total_ram_available = 0
    query = sql.SQL("""
        select total_ram
        from "TotalRAMInDisk"
        where "TotalRAMInDisk".disk_id = {diskID}
        """).format(
        diskID=sql.Literal(diskID))
    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            total_ram_available = entries.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()
    return total_ram_available


def getCostForDescription(description: str) -> int:
    cost = 0
    query = sql.SQL("""
        select sum("Disk".cost_per_byte * "Photo".disk_free_space_needed)
        from "Disk"
        inner join "PhotoInDisk" on "PhotoInDisk".disk_id = "Disk".id
        inner join "Photo" on "Photo".id = "PhotoInDisk".photo_id and "Photo".description = {description}
        """).format(
        description=sql.Literal(description))

    conn = None
    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query)
        if row_effected != 0:
            cost = entries.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()
    return cost

# clearTables()
# createTables()
# addDisk(Disk(1,"IBM", 100, 100000,1))
# addPhoto(Photo(1, "IBM", 100))
# addPhoto(Photo(2, "hagai", 100))
# addPhoto(Photo(5, "hagai", 55))
# addPhoto(Photo(7, "hagai", 3))
# addPhoto(Photo(3, "IBM", 100))
#
# addPhotoToDisk(Photo(1, "IBM", 100), 1)
# addPhotoToDisk(Photo(2, "hagai", 100), 1)
# addPhotoToDisk(Photo(5, "hagai", 55), 1)
# addPhotoToDisk(Photo(7, "hagai", 3), 1)
# addPhotoToDisk(Photo(3, "IBM", 100), 1)
# print(getCostForDescription("hagai"))
def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description: str, num: int) -> bool:
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
