from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from helperpy.core.type_annotations import (
    Number,
    NumberOrString,
)
from helperpy.core.random_data_generator import generate_random_hex_codes
from helperpy.core.utils import (
    get_max_of_abs_values,
    has_negative_number,
    has_positive_number,
)
from helperpy.data_wrangler.transform import linspace_by_index


def __reorder_timseries_data(
        x_vals: List[NumberOrString],
        y_vals: List[Number],
    ) -> pd.DataFrame:
    """
    Re-orders values of x-axis and y-axis (for timeseries chart data) in ascending order of x-axis' values.
    Returns DataFrame of re-ordered values. Columns returned are: `['x_vals', 'y_vals']`
    """
    if len(x_vals) != len(y_vals):
        raise ValueError(
            "Expected `x_vals` and `y_vals` to be of same length, but"
            f" got lengths {len(x_vals)} and {len(y_vals)} respectively"
        )
    data = pd.DataFrame(data={
        'x_vals': x_vals,
        'y_vals': y_vals,
    })
    data.sort_values(by='x_vals', ascending=True, ignore_index=True, inplace=True)
    return data


def __plot_descriptive_stats(
        array: List[Number],
        orient: str,
        line_style: Optional[str] = "-.",
        line_width: Optional[Number] = 2,
    ) -> Any:
    """
    Helper function to plot descriptive stats along one of the axes. Options for `orient`: ['v', 'h'].
    Returns matplotlib plot object that can be chained to more matplotlib code.
    """
    mean = round(np.mean(array), 3)
    median = round(np.median(array), 3)
    if orient == 'v':
        obj = plt.axvline(x=mean, label=f"Mean ({mean})", linestyle=line_style, linewidth=line_width, color='red')
        obj = plt.axvline(x=median, label=f"Median ({median})", linestyle=line_style, linewidth=line_width, color='green')
    elif orient == 'h':
        obj = plt.axhline(y=mean, label=f"Mean ({mean})", linestyle=line_style, linewidth=line_width, color='red')
        obj = plt.axhline(y=median, label=f"Median ({median})", linestyle=line_style, linewidth=line_width, color='green')
    else:
        raise ValueError(f"Expected `orient` to be in ['v', 'h'], but got '{orient}'")
    return obj


def get_calibrated_plot_fonts(fig_size: Tuple[int, int]) -> Dict[str, float]:
    """
    Takes in figure-size (tuple), and returns dictionary containing fontsizes for all other aspects
    of the plot (calculated and set appropriately, based on the figure-size).
    The fontsizes dictionary will have the following keys: `['title_size', 'label_size', 'tick_size', 'legend_label_size', 'annotation_size']`
    """
    if isinstance(fig_size, tuple):
        if len(fig_size) == 2:
            width = int(fig_size[0])
        else:
            raise ValueError (f"Invalid value for `fig_size`. Expected tuple of length 2, but got tuple of length {len(fig_size)}")
    else:
        raise TypeError(f"Invalid type for `fig_size`. Expected 'tuple' but got '{type(fig_size)}'")
    title_to_width_ratio = round((40 / 24), 3)
    labels_to_width_ratio = round((25 / 24), 3)
    ticks_to_width_ratio = round((20 / 24), 3)
    legend_labels_to_width_ratio = round((24 / 24), 3)
    annotation_to_width_ratio = round((18 / 24), 3)
    dictionary_calibrated_plot_fonts = {
        'title_size': width * title_to_width_ratio,
        'label_size': width * labels_to_width_ratio,
        'tick_size': width * ticks_to_width_ratio,
        'legend_label_size': width * legend_labels_to_width_ratio,
        'annotation_size': width * annotation_to_width_ratio,
    }
    return dictionary_calibrated_plot_fonts


def add_plot_skeleton(
        title: str,
        x_label: str,
        y_label: str,
        fig_size: Tuple[int, int],
        include_labels: Optional[bool] = True,
        include_ticks: Optional[bool] = True,
    ) -> Any:
    """Returns matplotlib plot object, containing skeleton of basic aspects of a plot"""
    dict_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
    obj = plt.figure(figsize=fig_size)
    obj = plt.title(title, fontsize=dict_plot_fonts['title_size'])
    if include_labels:
        obj = plt.xlabel(x_label, fontsize=dict_plot_fonts['label_size'])
        obj = plt.ylabel(y_label, fontsize=dict_plot_fonts['label_size'])
    if include_ticks:
        obj = plt.xticks(fontsize=dict_plot_fonts['tick_size'])
        obj = plt.yticks(fontsize=dict_plot_fonts['tick_size'])
    return obj


def plot_donut(
        title: str,
        labels: List[str],
        values: List[Number],
        colors: Optional[List[str]] = None,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    if not colors:
        colors = generate_random_hex_codes(how_many=len(labels))
    _, ax1 = plt.subplots()
    ax1.pie(x=list(values), labels=list(labels), colors=colors, autopct='%1.1f%%', startangle=40)
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    ax1.axis('equal')  
    plt.tight_layout()
    plt.title(title, fontsize=15)
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None


def plot_bar(
        title: str,
        x_label: str,
        y_label: str,
        horizontal: bool,
        bar_labels: List[str],
        bar_values: List[Number],
        fig_size: Optional[Tuple[int, int]] = (12, 5),
        colors: Optional[List[str]] = None,
        annotate: Optional[bool] = True,
        symmetrical: Optional[bool] = True,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    add_plot_skeleton(title=title,
                      x_label=x_label,
                      y_label=y_label,
                      fig_size=fig_size,
                      include_labels=True,
                      include_ticks=True)
    if not colors:
        colors = generate_random_hex_codes(how_many=len(bar_labels))
    if horizontal:
        plt.barh(y=bar_labels, width=bar_values, color=colors)
    else:
        plt.bar(x=bar_labels, height=bar_values, color=colors)
    if annotate:
        dict_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
        annotation_size = dict_plot_fonts['annotation_size']
    if annotate and horizontal:
        for idx, value in enumerate(bar_values):
            plt.text(x = value * 1.01,
                     y = idx,
                     s = str(value),
                     fontweight='bold',
                     fontsize=annotation_size,
                     color='black')
    if annotate and not horizontal:
        for idx, value in enumerate(bar_values):
            plt.text(x = idx,
                     y = value * 1.01,
                     s = str(value),
                     fontweight='bold',
                     fontsize=annotation_size,
                     color='black')
    if symmetrical:
        if has_negative_number(array=bar_values) and has_positive_number(array=bar_values):
            axis_limit = get_max_of_abs_values(array=bar_values) * 1.05
            if horizontal:
                plt.xlim(-axis_limit, axis_limit)
            else:
                plt.ylim(-axis_limit, axis_limit)
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None


def plot_valuecounts(
        title: str,
        array: List[Any],
        array_description: str,
        fig_size: Optional[Tuple[int, int]] = (12, 5),
        normalize: Optional[bool] = False,
        colors: Optional[List[str]] = None,
        symmetrical: Optional[bool] = True,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    series = pd.Series(data=array)
    if normalize:
        dict_valuecounts = series.value_counts(normalize=True).mul(100).apply(round, args=[2]).to_dict()
        x_label = "Percentage of occurences"
    else:
        dict_valuecounts = series.value_counts().to_dict()
        x_label = "Count of occurences"
    plot_bar(title=title,
             x_label=x_label,
             y_label=array_description,
             horizontal=True,
             bar_labels=list(dict_valuecounts.keys()),
             bar_values=list(dict_valuecounts.values()),
             fig_size=fig_size,
             colors=colors,
             annotate=False,
             symmetrical=symmetrical,
             save_at=save_at,
             show=show)
    return None


def plot_timeseries(
        title: str,
        x_label: str,
        y_label: str,
        x_vals: List[Any],
        y_vals: List[Number],
        trim_xticks: Optional[bool] = True,
        fig_size: Optional[Tuple[int, int]] = (12, 5),
        area_fill: Optional[bool] = False,
        area_color: Optional[str] = "skyblue",
        area_alpha: Optional[float] = 0.4,
        line_width: Optional[Number] = 3,
        line_color: Optional[str] = "#135FB6",
        show_scatter_points: Optional[bool] = False,
        scatter_points_size: Optional[int] = 250,
        describe: Optional[bool] = False,
        symmetrical: Optional[bool] = True,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    """Note: Expects `x_vals` to denote time-related data, and `y_vals` to denote the timeseries curve/line itself"""
    add_plot_skeleton(title=title,
                      x_label=x_label,
                      y_label=y_label,
                      fig_size=fig_size,
                      include_labels=True,
                      include_ticks=False)
    x_vals, y_vals = list(x_vals), list(y_vals)
    df_timeseries_data = __reorder_timseries_data(x_vals=x_vals, y_vals=y_vals)
    if area_fill:
        plt.fill_between(df_timeseries_data['x_vals'],
                         df_timeseries_data['y_vals'],
                         color=area_color,
                         alpha=area_alpha)
    plt.plot(df_timeseries_data['x_vals'],
             df_timeseries_data['y_vals'],
             color=line_color,
             linewidth=line_width)
    xticks = df_timeseries_data['x_vals'].tolist()
    if trim_xticks:
        xticks = linspace_by_index(array=xticks, how_many=5)
    dict_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
    tick_size = dict_plot_fonts['tick_size']
    plt.xticks(xticks, fontsize=tick_size)
    plt.yticks(fontsize=tick_size)
    if symmetrical:
        y_vals = df_timeseries_data['y_vals'].tolist()
        if has_negative_number(array=y_vals) and has_positive_number(array=y_vals):
            axis_limit = get_max_of_abs_values(array=y_vals) * 1.05
            plt.ylim(-axis_limit, axis_limit)
    if show_scatter_points:
        plt.scatter(df_timeseries_data['x_vals'],
                    df_timeseries_data['y_vals'],
                    color="black",
                    s=scatter_points_size,
                    marker='*',
                    label=None)
    if describe:
        __plot_descriptive_stats(array=y_vals, orient='h', line_style="-.", line_width=line_width)
    if describe: # Showing legend
        dict_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
        legend_label_size = dict_plot_fonts['legend_label_size']
        plt.legend(loc='best', fontsize=legend_label_size)
    plt.grid()
    plt.tight_layout()
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None


def plot_scatter(
        title: str,
        x_label: str,
        y_label: str,
        x_vals: List[Number],
        y_vals: List[Number],
        fig_size: Optional[Tuple[int, int]] = (12, 5),
        color: Optional[str] = 'g',
        alpha: Optional[float] = None,
        size: Optional[int] = 80,
        grid: Optional[bool] = True,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    add_plot_skeleton(title=title,
                      x_label=x_label,
                      y_label=y_label,
                      fig_size=fig_size,
                      include_labels=True,
                      include_ticks=True)
    plt.scatter(x=list(x_vals), y=list(y_vals), c=color, s=size, alpha=alpha)
    if grid:
        plt.grid()
    plt.tight_layout()
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None


def plot_histogram(
        title: str,
        array: List[Number],
        array_description: str,
        fig_size: Optional[Tuple[int, int]] = (12, 5),
        bins: Optional[int] = 10,
        color: Optional[str] = "#2B88CA",
        describe: Optional[bool] = False,
        line_width: Optional[Number] = 3,
        grid: Optional[bool] = True,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    add_plot_skeleton(title=title,
                      x_label=array_description,
                      y_label="Frequency",
                      fig_size=fig_size,
                      include_labels=True,
                      include_ticks=True)
    plt.hist(x=array, bins=bins, color=color)
    if describe:
        __plot_descriptive_stats(array=array, orient='v', line_style="-.", line_width=line_width)
    if describe: # Showing legend
        dict_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
        legend_label_size = dict_plot_fonts['legend_label_size']
        plt.legend(loc='best', fontsize=legend_label_size)
    if grid:
        plt.grid()
    plt.tight_layout()
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None


def plot_radar(
        title: str,
        labels: List[str],
        values: List[Number],
        ticks: Optional[List[Number]] = None,
        tick_limit: Optional[Tuple[Number, Number]] = None,
        fig_size: Optional[Tuple[int, int]] = (15, 9),
        color: Optional[str] = None,
        save_at: Optional[str] = None,
        show: Optional[bool] = False,
    ) -> None:
    dict_calibrated_plot_fonts = get_calibrated_plot_fonts(fig_size=fig_size)
    angles = np.linspace(start=0, stop=2*np.pi, num=len(labels), endpoint=False)
    values = np.concatenate((values, [values[0]]))
    labels = np.concatenate((labels, [labels[0]]))
    angles = np.concatenate((angles, [angles[0]]))
    
    fig = plt.figure(figsize=fig_size)
    ax = fig.add_subplot(111, polar=True)
    ax.plot(angles, values, 'o-', linewidth=2.5, c=color)
    ax.fill(angles, values, alpha=0.35, c=color)
    ax.set_thetagrids(angles * 180 / np.pi, labels, size=dict_calibrated_plot_fonts['label_size'])
    ax.grid(True)
    if ticks:
        plt.yticks(ticks, size=dict_calibrated_plot_fonts['tick_size'])
    else:
        plt.yticks([])
    if tick_limit:
        plt.ylim(tick_limit)
    plt.title(title, size=dict_calibrated_plot_fonts['title_size'])
    if save_at:
        plt.savefig(save_at)
    if show:
        plt.show()
    plt.close()
    return None