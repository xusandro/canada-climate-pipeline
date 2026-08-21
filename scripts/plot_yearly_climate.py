"""Plot the yearly climate mart: mean TMAX above, reporting stations below.

Two stacked panels sharing the year axis, deliberately not one chart with two
y-scales: the point is that the temperature series cannot be read without the
station count beside it, and a second scale would invite comparing their shapes
as if they were the same measure.

    python scripts/plot_yearly_climate.py docs/yearly_climate.csv docs/temperature_trend.png
"""

import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_MUTED = "#52514e"
TEMP = "#2a78d6"
STATIONS = "#eb6834"

# 2024 lost its temperature feed from May onward and 2025 is still filling in,
# so both are drawn but marked as unusable for a trend (see ADR-011).
INCOMPLETE_FROM = 2024


def read_rows(path):
    with open(path, newline="") as f:
        rows = [r for r in csv.DictReader(f) if r.get("avg_tmax_c")]
    rows.sort(key=lambda r: int(r["observation_year"]))
    years = [int(r["observation_year"]) for r in rows]
    tmax = [float(r["avg_tmax_c"]) for r in rows]
    stations = [int(r["station_count"]) for r in rows]
    return years, tmax, stations


def annotate_low_point(ax, years, values, label):
    year = min(zip(values, years))[1]
    value = values[years.index(year)]
    ax.plot([year], [value], "o", ms=8, color=TEMP, zorder=3)
    ax.annotate(
        label,
        xy=(year, value),
        xytext=(-12, 26),
        textcoords="offset points",
        ha="right",
        fontsize=9,
        color=INK,
        arrowprops={"arrowstyle": "-", "color": INK_MUTED, "lw": 1},
    )


def style(ax):
    ax.set_facecolor(SURFACE)
    ax.grid(axis="y", color="#e6e5e1", lw=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color("#d6d5d0")
    ax.tick_params(colors=INK_MUTED, length=0, labelsize=9)


def main(csv_path, out_path):
    years, tmax, stations = read_rows(csv_path)

    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(9, 6), sharex=True, gridspec_kw={"hspace": 0.28}
    )
    fig.patch.set_facecolor(SURFACE)

    top.plot(years, tmax, lw=2, color=TEMP)
    top.set_title(
        "Mean daily maximum temperature, Canadian GHCN stations",
        loc="left", fontsize=12, color=INK, pad=10,
    )
    top.set_ylabel("°C", color=INK_MUTED, fontsize=9)
    annotate_low_point(top, years, tmax, f"{INCOMPLETE_FROM}: temperature feed\nstopped in May")

    bottom.plot(years, stations, lw=2, color=STATIONS)
    bottom.set_title(
        "Stations reporting that year",
        loc="left", fontsize=12, color=INK, pad=10,
    )
    bottom.set_ylabel("stations", color=INK_MUTED, fontsize=9)
    bottom.set_ylim(0, max(stations) * 1.15)
    # Label the first year and the last *complete* one. Labelling the final row
    # would quote a decline that is partly just a year still being reported.
    for year in (years[0], INCOMPLETE_FROM - 1):
        value = stations[years.index(year)]
        bottom.annotate(
            f"{value:,}",
            xy=(year, value), xytext=(0, 10), textcoords="offset points",
            ha="center", fontsize=9, color=INK,
        )

    for ax in (top, bottom):
        style(ax)
        ax.axvspan(INCOMPLETE_FROM - 0.5, years[-1] + 0.5, color="#000000", alpha=0.05, lw=0)

    bottom.set_xlabel("")
    fig.text(
        0.01, 0.015,
        "Shaded years are incomplete and must not carry a trend. "
        "Averages are taken over a shifting station population.",
        fontsize=8, color=INK_MUTED,
    )

    fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor=SURFACE)
    print(f"wrote {out_path}  ({years[0]}-{years[-1]}, {len(years)} years)")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
