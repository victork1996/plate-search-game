import sqlite3
import argparse
import math


def get_total_record_count(records_db_path):
    with sqlite3.connect(records_db_path) as db:
        (total_count,) = db.execute("SELECT COUNT (`plate_number`) FROM `records`;").fetchone()
    return total_count


def get_latest_production_year_and_appearance_count(db, number):
    """
    Returns the latest production year of a car with the specified number in its plate and the number's
     appearance count.
    """
    return db.execute("SELECT MAX(`production_year`), COUNT(*) from `records` WHERE `first`=:1 OR "
                      "second=:1 OR `third`=:1;", (number,)).fetchone()


def _get_sorted_counts(db):
    """
    Returns a sorted list of the appearance counts of each number in the Israeli plates.
    """
    numbers = []
    for i in range(1000):
        appearance_count = db.execute("SELECT COUNT(*) from `records` WHERE `first`=:1 OR `second`=:1 OR `third`=:1",
                                      (i, )).fetchone()[0]
        numbers.append(appearance_count)

    numbers.sort(reverse=True)
    return numbers


def estimate_difficulty_level(records_db_path, number_to_check):
    """
    Estimates the difficulty level of finding a number in Israeli plates.

    :param records_db_path: a path to a DB containing records of israeli plates
    :param number_to_check: the number of which to estimate the difficulty
    """
    if number_to_check < 0 or number_to_check > 999:
        raise ValueError("Number should be between 0 and 999")

    with sqlite3.connect(records_db_path) as db:
        sorted_appearance_count = _get_sorted_counts(db)
        _, appearance_count = get_latest_production_year_and_appearance_count(db, number_to_check)
        return math.floor((sorted_appearance_count.index(appearance_count)) / len(sorted_appearance_count) * 100) + 1


def check_number_rarity(records_db_path, number_to_check):
    """
    Checks the rarity of the specified number in israeli plates.

    :param records_db_path: a path to a DB containing records of israeli plates.
    :param number_to_check: the number to check.
    """
    if number_to_check < 0 or number_to_check > 999:
        raise ValueError("Number should be between 0 and 999")

    with sqlite3.connect(records_db_path) as db:
        latest_production_year, appearance_count = get_latest_production_year_and_appearance_count(db, number_to_check)

    return appearance_count, latest_production_year


def main(args):
    total_count = get_total_record_count(args.records_db_path)
    appearance_count, latest_production_year = check_number_rarity(args.records_db_path, int(args.number_to_check))
    print(f"The number {args.number_to_check} appears on {appearance_count:,}"
          f" plates out of a total of {total_count:,}.")

    print(f"The chance to find it is 1 in {int(total_count / appearance_count)}.")
    print(f"The latest production year of a car with that number is {latest_production_year}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("records_db_path")
    parser.add_argument("number_to_check")
    main(parser.parse_args())
