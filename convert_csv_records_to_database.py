import sqlite3
import argparse

_VALUE_SEPARATOR = '|'  # The file is not really COMMA separated values, but rather a '|' separated values.
_VALUE_WRAPPER = '"'  # Each value is wrapped with this


def create_memory_db():
    """
    Creates a temporary in-memory database that will be used during the conversion.
    """
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE `records` (`production_year` INTEGER , `plate_number` INTEGER, `first` INTEGER, "
               "`second` INTEGER, `third` INTEGER);")

    # Create indexes for super fast queries.
    db.execute('CREATE INDEX first_part_index on records(first);')
    db.execute('CREATE INDEX second_part_index on records(second);')
    db.execute('CREATE INDEX third_part_index on records(third);')
    return db


def split_plate_into_parts(plate_number):
    """
    Splits the plate number into parts.
    The length of each part depends on the length of the plate number itself.
    """
    if len(plate_number) == 7:
        return int(plate_number[:2]), int(plate_number[2:-2]), int(plate_number[-2:])
    elif len(plate_number) == 8:
        return int(plate_number[:3]), int(plate_number[3:-3]), int(plate_number[-3:])


def insert_csv_line_into_db(line, db):
    """
    Inserts a single CSV record into the DB. For now, we're interested only in the record's plate number.
    """
    values = line.split(_VALUE_SEPARATOR)
    plate_number = values[0].strip(_VALUE_WRAPPER)
    production_year = int(values[9].strip(_VALUE_WRAPPER))

    # Plate numbers don't start with 0's. Any record that starts with a 0 is actually a 7 digit plate number.
    plate_number = plate_number.lstrip("0")
    first, second, third = split_plate_into_parts(plate_number)
    db.execute("INSERT INTO `records` VALUES (?, ?, ?, ?, ?)", (production_year, plate_number, first, second, third))


def write_output_db_file(src_db, output_db_path):
    """
    Copies the entire source database to a database on the hard-drive.

    :param src_db: the database to copy
    :param output_db_path: where to store the output database on the hard-drive.
    """
    with sqlite3.connect(output_db_path) as output_db:
        src_db.backup(output_db)


def convert_csv_file_to_db(csv_file_path, output_db_path):
    """
    Converts a csv file of car records to an sqlite3 database.
    """
    with create_memory_db() as db:
        with open(csv_file_path, 'r') as csv_records_file:
            # Skip the first line, it contains headers
            csv_records_file.readline()

            # Parse all the records
            while True:
                line = csv_records_file.readline()
                if not line:
                    break

                insert_csv_line_into_db(line, db)

        db.commit()
        write_output_db_file(db, output_db_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file_path")
    parser.add_argument("output_db_path")
    args = parser.parse_args()
    convert_csv_file_to_db(args.csv_file_path, args.output_db_path)


if __name__ == '__main__':
    main()
