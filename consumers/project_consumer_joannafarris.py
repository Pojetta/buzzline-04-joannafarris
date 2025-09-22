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

# Slow things way down so terminal output is readable
VISUAL_PAUSE_SECS = 0   # pause after each processed message
IDLE_POLL_SECS   = 0    # wait this long when no new messages

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE   = DATA_FOLDER.joinpath("project_live.json")

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

PALETTE = [
    "#ff7e0ec2",  # vivid orange
    "#d62728",  # red
    "#9467bd",  # purple
    "#e6550d",  # orange-red
    "#cc4778",  # magenta
    "#8e0152",  # crimson-magenta
    "#efd511",  # deep orange-brown
    "#4b1a54",  # deep purple
    "#e51082b7",  # fuchsia
    "#f77e136f",  # burnt orange
    "#e7bb0b",  # wine
]

keyword_color = {}
def color_for(kw: str):
    if kw not in keyword_color:
        keyword_color[kw] = PALETTE[len(keyword_color) % len(PALETTE)]
    return keyword_color[kw]

# Legend order control (False = bottom→top, True = top→bottom)
LEGEND_TOP_FIRST = False


def update_chart_all():
    ax.clear()

    authors = sorted(keyword_counts_by_author.keys())
    if not authors:
        plt.draw(); plt.pause(0.01)
        return

    # union of all keywords across authors
    all_keywords = set()
    for a in authors:
        all_keywords.update(keyword_counts_by_author[a].keys())

    # order keywords (put "(none)" last if present)
    keywords = sorted([k for k in all_keywords if k != "(none)"])
    if "(none)" in all_keywords:
        keywords.append("(none)")

    x = list(range(len(authors)))
    bottoms = [0] * len(authors)

    for kw in keywords:
        heights = [keyword_counts_by_author[a].get(kw, 0) for a in authors]
        ax.bar(x, heights, bottom=bottoms, label=kw, color=color_for(kw), edgecolor="black", linewidth=0.3)
        # update bottoms element-wise
        bottoms = [b + h for b, h in zip(bottoms, heights)]

    ax.set_xlabel("Authors")
    ax.set_ylabel("Keyword Counts")
    ax.set_title("Real-Time Keyword Frequency per Author (Stacked)")
    ax.set_xticks(x)
    ax.set_xticklabels(authors, rotation=45, ha="right")

    # Legend that matches the stack order
    kw_order = keywords if not LEGEND_TOP_FIRST else list(reversed(keywords))
    handles = [Patch(facecolor=color_for(kw), edgecolor="black", label=kw) for kw in kw_order]
    ax.legend(handles=handles, title="Keywords",
              loc="upper left", bbox_to_anchor=(1.02, 1), borderaxespad=0)

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

    # 4) Update counts (per author)
    keyword_counts_by_author[author][keyword] += 1
    print("COUNTS (this author):", dict(keyword_counts_by_author[author]))

    # 5) One chart for ALL authors
    print("Calling update_chart()...")
    update_chart_all()
    print("Chart updated.\n")

    # 6) Slow down so you can read
    #time.sleep(VISUAL_PAUSE_SECS)


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
