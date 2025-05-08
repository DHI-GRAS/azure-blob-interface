from pathlib import Path
import re


def get_prefix(product_filename, product_type, aoi=""):
    out_path = None
    if product_type.lower() == "s2":
        out_path = get_s2_path(*parse_s2_path(product_filename))
    if product_type.lower() == "s3":
        out_path = get_s3_path(aoi, *parse_s3_path(product_filename))

    if not out_path:
        raise ProductTypeNotSupported("{product_type} product type not supported")

    return out_path


def parse_s2_path(s2_path):
    level, year, month, day, tile = re.search(
        "S2[ABCD]_MSI(L[12][AC])_(\d{4})(\d{2})(\d{2})T\d{6}_.*_.*_(T\d{2}\D{3})_",
        str(s2_path),
    ).groups()
    return level, tile, int(year), int(month), int(day)


def get_s2_path(level, tile, year, month, day):
    return (
        Path("Sentinel-2")
        / level
        / tile
        / "{}".format(year)
        / "{:02d}".format(month)
        / "{:02d}".format(day)
    )


def parse_s3_path(s3_path):
    sensor, level, year, month, day = re.search(
        "S3[ABCD]_([A-Z]{2})_(\d)_[A-Z]{3}____(\d{4})(\d{2})(\d{2})T\d{6}_",
        str(s3_path),
    ).groups()
    return sensor, level, int(year), int(month), int(day)


def get_s3_path(aoi, sensor, level, year, month, day):
    if sensor == "OL":
        sensor = "OLCI"
    elif sensor == "SL":
        sensor = "SLSTR"
    elif sensor == "SY":
        sensor = "SYNERGY"
    if level[0] != "L":
        level = "L%s" % level

    return (
        Path("Sentinel-3")
        / sensor
        / level
        / aoi
        / "{}".format(year)
        / "{:02d}".format(month)
        / "{:02d}".format(day)
    )


def parse_landsat_path(landsat_path):
    collection, year, month, day = re.search(
        "L[CE]0\d_([A-Z0-9]*)_\d{6}_(\d{4})(\d{2})(\d{2})_",
        str(landsat_path),
    ).groups()
    return collection, int(year), int(month), int(day)


def get_landsat_path(aoi, collection, year, month, day):
    return (
        Path("Landsat")
        / collection
        / aoi
        / "{}".format(year)
        / "{:02d}".format(month)
        / "{:02d}".format(day)
    )


class ProductTypeNotSupported(Exception):
    pass
