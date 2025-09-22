"""
project_consumer_joannafarris.py

Read a JSON-formatted file as it is being written and visualize
keyword frequency per author in real time (stacked bar chart).
"""

#####################################
# Imports
#####################################

import json
import os
import sys
import time
import pathlib
from collections import defaultdict
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.colors import ListedColormap
from utils.utils_logger import logger


#####################################
# Config & Paths
#####################################

# Speed settings
VISUAL_PAUSE_SECS = 0.0  # pause after each processed message
IDLE_POLL_SECS = 0.0     # wait this long when no new messages

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")


#####################################
# Data: author -> { keyword -> count }
#####################################

keyword_counts_by_author = defaultdict(lambda: defaultdict(int))


#####################################
# Live Plot (stacked bars: authors x, stacks = keywords)
#####################################

plt.ion()
fig, ax = plt.subplots()

# --- Distinct warm palette (teal/indigo/magenta/coral/amber) ---
PALETTE = [
    "#134E6F",    # deep teal
    "#134D6FC7",  # teal (with alpha)
    "#6064A8",    # indigo
    "#6064A8C1",  # light indigo (with alpha)
    "#C85C9FB2",  # magenta (with alpha)
    "#E76F6A",    # coral red
    "#E76E6AB4",  # light coral red (with alpha)
    "#F2B24D",    # amber
    "#F2B34D84",  # light amber (with alpha)
]

# Color assignment + stable palette order tracking
keyword_color: dict[str, str] = {}  # kw -> hex color
assign_rank: dict[str, int] = {}    # kw -> 0,1,2... in palette order


def color_for(kw: str):
    """Assign a color the first time we see kw, recording its palette order."""
    if kw not in keyword_color:
        idx = len(keyword_color)  # 0,1,2...
        keyword_color[kw] = PALETTE[idx % len(PALETTE)]
        assign_rank[kw] = len(assign_rank)
    return keyword_color[kw]


# Legend order control (False = bottom→top, True = top→bottom)
LEGEND_TOP_FIRST = True


def update_chart_all():
    ax.clear()

    authors = sorted(keyword_counts_by_author.keys())
    if not authors:
        plt.draw()
        plt.pause(0.01)
        return

    # union of all keywords across authors
    all_keywords = set()
    for a in authors:
        all_keywords.update(keyword_counts_by_author[a].keys())

    # ensure any brand-new keywords get assigned a color/rank now
    for kw in all_keywords:
        _ = color_for(kw)

    # order keywords by first-assigned palette order; put "(none)" last
    keywords = [k for k in all_keywords if k != "(none)"]
    keywords.sort(key=lambda k: (assign_rank.get(k, 10**9), k))
    if "(none)" in all_keywords:
        keywords.append("(none)")

    x = list(range(len(authors)))
    bottoms = [0] * len(authors)

    for kw in keywords:
        heights = [keyword_counts_by_author[a].get(kw, 0) for a in authors]
        ax.bar(
            x,
            heights,
            bottom=bottoms,
            label=kw,
            color=color_for(kw),
            edgecolor="white",
            linewidth=0.6,
        )
        # update bottoms element-wise
        bottoms = [b + h for b, h in zip(bottoms, heights)]

    ax.set_xlabel("Authors")
    ax.set_ylabel("Keyword Counts")
    ax.set_title("Real-Time Keyword Frequency per Author (Stacked)")
    ax.set_xticks(x)
    ax.set_xticklabels(authors, rotation=45, ha="right")

    # Legend that matches the stack order (same as keywords order)
    kw_order = keywords if not LEGEND_TOP_FIRST else list(reversed(keywords))
    handles = [Patch(facecolor=color_for(kw), edgecolor="black", label=kw) for kw in kw_order]
    ax.legend(
        handles=handles,
        title="Keywords",
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        borderaxespad=0,
    )

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)


#####################################
# Process one message
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.
    Expected fields: 'author', 'keyword_mentioned'
    """
    # 1) RAW
    print("RAW:", message.strip())

    # 2) Parse
    try:
        message_dict = json.loads(message)
    except json.JSONDecodeError:
        print("BAD JSON, skipping:", message.strip())
        return

    print("PARSED:", message_dict)

    # 3) Fields
    author = message_dict.get("author", "unknown")
    keyword = message_dict.get("keyword_mentioned") or "(none)"
    print(f"FIELDS → author={author}, keyword={keyword}")

    # ensure keyword gets a color/rank at first sight
    _ = color_for(keyword)

    # 4) Update counts (per author)
    keyword_counts_by_author[author][keyword] += 1
    print("COUNTS (this author):", dict(keyword_counts_by_author[author]))

    # 5) One chart for ALL authors
    print("Calling update_chart()...")
    update_chart_all()
    print("Chart updated.\n")

    # 6) (optional) throttle
    if VISUAL_PAUSE_SECS:
        time.sleep(VISUAL_PAUSE_SECS)


#####################################
# Main loop (tail a file)
#####################################

def main() -> None:
    logger.info("START consumer.")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                line = file.readline()

                if line.strip():
                    process_message(line)
                else:
                    if IDLE_POLL_SECS:
                        time.sleep(IDLE_POLL_SECS)
                    continue

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
