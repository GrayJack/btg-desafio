from pathlib import Path
from typing import Optional
from shapely import Point, Polygon
from pandas import DataFrame, Timestamp
import pandas as pd
import re


def read_data_file(file_path: str | Path) -> pd.DataFrame:
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


def apply_contour(contour_df: pd.DataFrame, data_df: pd.DataFrame) -> pd.DataFrame:
    contour = Polygon(contour_df.values)

    inside_contour = []
    for lat, long, val in data_df.itertuples(index=False, name=None):
        if contour.contains(Point(lat, long)):
            inside_contour.append([lat, long, val])

    return pd.DataFrame(inside_contour, columns=["lat", "long", "data_value"])


def get_dates_from_eta_filename(filename: str) -> Optional[tuple[Timestamp, Timestamp]]:
    if filename.startswith("ETA40_p"):
        forecast_day = int(filename[7:9])
        forecast_mon = int(filename[9:11])
        forecast_yr = 2000 + int(filename[11:13])

        forecasted_day = int(filename[14:16])
        forecasted_mon = int(filename[16:18])
        forecasted_yr = 2000 + int(filename[18:20])

        forecast_date = Timestamp(year=forecast_yr, month=forecast_mon, day=forecast_day)
        forecasted_date = Timestamp(year=forecasted_yr, month=forecasted_mon, day=forecasted_day)

        return (forecast_date, forecasted_date)

    return None


def main() -> None:
    contour_df: pd.DataFrame = read_contour_file("PSATCMG_CAMARGOS.bln")
    files = Path("forecast_files").glob("*.dat")
    acumuladas = []

    for filepath in files:
        timestamps = get_dates_from_eta_filename(filepath.name)
        # ignore wrong files
        if timestamps is None:
            continue

        forecast_date, forecasted_date = timestamps

        data_df = read_data_file(filepath)
        processed_df = apply_contour(contour_df, data_df)
        acumulada = processed_df["data_value"].sum()

        acumuladas.append([forecast_date, forecasted_date, acumulada])

    acumuladas_df = DataFrame(
        acumuladas,
        columns=[
            "forecast_date",
            "forecasted_date",
            "accumulated preciptation",
        ],
    )

    print(acumuladas_df)

    ax = acumuladas_df.loc[:, ["forecasted_date", "accumulated preciptation"]].plot.line(
        y="accumulated preciptation",
        x="forecasted_date",
        xlabel="Forecasted Date",
        ylabel="Accumulated Preciptation (mm)",
        title="Expected Accumulated Preciptation for Camargos - Bacia Grande",
        legend=False,
        linewidth=4,
    )
    ax.grid(True, linewidth=0.25)


if __name__ == "__main__":
    main()
