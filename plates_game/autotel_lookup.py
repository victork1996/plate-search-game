from typing import NamedTuple, Dict, Set
import argparse
import json
import re
import urllib.request

_AUTOTEL_MAP_PAGE_URL = "https://www.autotel.co.il/assets/map/maptest.php?lang=he"


class AutoTelCar(NamedTuple):
    id: int
    license_plate: str  # Parts of the license plate are separated by '-'.
    address: str

    def __hash__(self):
        return hash(self.license_plate)

    @staticmethod
    def from_map_json(car_dict):
        return AutoTelCar(
            id=int(car_dict["nickname"]),
            license_plate=car_dict["licencePlate"],
            address=car_dict["addressHe"]
        )


def _get_autotel_map_page() -> str:
    """
    Returns the source of AutoTel's map.
    """

    f = urllib.request.urlopen(_AUTOTEL_MAP_PAGE_URL)
    return f.read().decode("utf-8")


def _get_cars_map_data_variable(page_data: str) -> dict:
    """
    Extracts the car data from AutoTel's map page.
    The page contains a JavaScript variable. This function finds it and parses its contents.

    :param page_data: The source of AutoTel's map page.
    """

    json_match = re.search(r"(var cars = )(?P<data>\[.*\]);", page_data)
    assert json_match is not None
    json_data = json_match["data"]
    return json.loads(json_data)


def _create_license_plate_mapping(cars_map_data: dict) -> Dict[int, Set[AutoTelCar]]:
    """
    Creates a mapping between a license plate portion number and AutoTel cars with that number.
    For example, a car with the license plate '12-345-67' will be included under the keys 12, 345 and 67.

    :param cars_map_data: The (parsed) map data from AutoTel's map page.
    """

    plate_map = {n: set() for n in range(1000)}
    for entry in cars_map_data:
        items = entry["items"]
        if len(items) > 0:
            for car in (AutoTelCar.from_map_json(raw_car_data) for raw_car_data in items.values()):
                license_plate_numbers = [int(num) for num in car.license_plate.split('-')]
                for number in license_plate_numbers:
                    plate_map[number].add(car)

    return plate_map


def get_autotel_license_plate_mapping() -> Dict[int, Set[AutoTelCar]]:
    """
    Requests AutoTel's map page and returns a mapping between a license plate portion number and AutoTel cars with
    that number.
    """

    page_data = _get_autotel_map_page()
    cars_map_data = _get_cars_map_data_variable(page_data)
    return _create_license_plate_mapping(cars_map_data)


def get_summary_for_autotel_cars_with_number(num: int):
    """
    Returns a nicely formatted string showing how many AutoTel cars with the given number exist, and where they are
    parking.

    :param num: The number to look for.
    """

    if num < 0 or num > 999:
        raise IndexError(f"License plate number {num} is out of range")

    cars = get_autotel_license_plate_mapping()[num]
    car_count = len(cars)
    if car_count == 0:
        return "No cars were found."

    lines = ["One car was found:" if car_count == 1 else f"{car_count} cars were found:"]
    for car in cars:
        lines.append(f'With license plate {car.license_plate} (car id {car.id}) at this address:')
        lines.append(car.address)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("number", type=int)
    args = parser.parse_args()
    print(get_summary_for_autotel_cars_with_number(args.number))


if "__main__" == __name__:
    main()
