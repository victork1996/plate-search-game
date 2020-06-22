import sqlite3
import argparse


def get_total_record_count(db):
    (total_count,) = db.execute("SELECT COUNT (`plate_number`) FROM `records`;").fetchone()
    return total_count


def get_latest_production_year_and_appearance_count(db, number):
    """
    Returns the latest production year of a car with the specified number in its plate and the number's
     appearance count.
    """
    return db.execute("SELECT MAX(`production_year`), COUNT(*) from `records` WHERE `first`=:1 OR "
                      "second=:1 OR `third`=:1;", (number,)).fetchone()


def check_number_rarity(records_db_path, number_to_check):
    """
    Checks the rarity of the specified number in israeli plates.

    :param records_db_path: a path to a DB containing records of israeli plates.
    :param number_to_check: the number to check.
    """
    with sqlite3.connect(records_db_path) as db:
        total_count = get_total_record_count(db)
        latest_production_year, appearance_count = get_latest_production_year_and_appearance_count(db, number_to_check)

    print(f"The number {number_to_check} appears on {appearance_count} plates out of a total of {total_count:,}.")
    print(f"The chance to find it is 1 in {total_count/appearance_count}.")
    print(f"The latest production year of a car with that number is {latest_production_year}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("records_db_path")
    parser.add_argument("number_to_check")
    args = parser.parse_args()

    check_number_rarity(args.records_db_path, args.number_to_check)


if __name__ == "__main__":
    main()
