import pandas as pd
import re
import time
from functools import wraps


def read_data_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        raw_file = f.readlines()

    list_dados = [line.split() for line in raw_file]
    float_raw_lines = [list(map(float, raw_line)) for raw_line in list_dados]
    return pd.DataFrame(float_raw_lines, columns=["lat", "long", "data_value"])


def read_contour_file(file_path: str) -> pd.DataFrame:
    line_split_comp = re.compile(r"\s*,")

    with open(file_path, "r") as f:
        raw_file = f.readlines()

    l_raw_lines = [line_split_comp.split(raw_file_line.strip()) for raw_file_line in raw_file]
    l_raw_lines = list(filter(lambda item: bool(item[0]), l_raw_lines))
    float_raw_lines = [list(map(float, raw_line))[:2] for raw_line in l_raw_lines]
    header_line = float_raw_lines.pop(0)
    assert len(float_raw_lines) == int(header_line[0])
    return pd.DataFrame(float_raw_lines, columns=["lat", "long"])


def is_inside_contour(edges, lat: float, long: float) -> bool:
    # Checking if the point is inside the contour by casting a ray at one side from the
    # `(lat, long)` point and counting if number of times the line crosses the polygon.
    # If it's odd, the point is inside, otherwise it is outside.
    #
    # The case is always true independent of the ray direction. So for simplicity, we
    # are always casting a ray to the left of the point.
    cnt = 0
    for edge in edges:
        (lat1, long1), (lat2, long2) = edge
        # The point must be between the values of the longitude of the edge (Point pair)
        cond1 = (long < long1) != (long < long2)
        # The point must be:
        # - The latitude value must be lower than both latitude values of the edge to be inside  # noqa: E501
        # - The latitude value must bigger than both latitude values of the edge to be outside  # noqa: E501
        # - If it is between the both latitude values of the edge:
        #   Consider lat_zero the latitude value of the exact point where the ray crosses the edge line  # noqa: E501
        #   - It is inside if the lat is before lat_zero
        #   - It is outside if the lat is before lat_zero
        #
        # Obs.: `+ 0.00000001` is used to avoid division by zero
        #
        # The calculation of the lat_zero and check the validity can be simplified by
        # the equation used below
        cond2 = lat < lat1 + ((long - long1) / (long2 - long1 + 0.00000001)) * (lat2 - lat1)

        if cond1 and cond2:
            cnt += 1

    return cnt % 2 == 1


def apply_contour(contour_df: pd.DataFrame, data_df: pd.DataFrame) -> pd.DataFrame:
    contour_list = list(contour_df.itertuples(index=False, name=None))
    list_edges = list(zip(contour_list, contour_list[1:] + contour_list[:1]))

    inside_contour = []
    for lat, long, val in data_df.itertuples(index=False, name=None):
        if is_inside_contour(edges=list_edges, lat=lat, long=long):
            inside_contour.append([lat, long, val])

    return pd.DataFrame(inside_contour, columns=["lat", "long", "data_value"])


def main() -> None:
    contour_df: pd.DataFrame = read_contour_file("PSATCMG_CAMARGOS.bln")
    data_df: pd.DataFrame = read_data_file("forecast_files/ETA40_p011221a021221.dat")
    contour_df: pd.DataFrame = apply_contour(contour_df=contour_df, data_df=data_df)


if __name__ == "__main__":
    main()
