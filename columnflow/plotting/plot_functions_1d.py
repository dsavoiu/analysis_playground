# coding: utf-8

"""
Example plot functions for one-dimensional plots.
"""

from __future__ import annotations

from collections import OrderedDict

import law

from columnflow.types import Iterable
from columnflow.util import maybe_import
from columnflow.plotting.plot_all import plot_all
from columnflow.plotting.plot_util import (
    prepare_plot_config,
    prepare_style_config,
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def plot_variable_per_process(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    plot_config = prepare_plot_config(
        hists,
        shape_norm=shape_norm,
        hide_errors=hide_errors,
    )

    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_variable_variants(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool = False,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_density_to_hists(hists, density)

    plot_config = OrderedDict()

    # for updating labels of individual selector steps
    selector_step_labels = config_inst.x("selector_step_labels", {})

    # add hists
    for label, h in hists.items():
        norm = sum(h.values()) if shape_norm else 1
        plot_config[f"hist_{label}"] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": norm,
                "label": selector_step_labels.get(label, label),
            },
            "ratio_kwargs": {
                "norm": hists["Initial"].values(),
            },
        }
        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    # setup style config
    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )
    # plot-function specific changes
    default_style_config["rax_cfg"]["ylim"] = (0., 1.1)
    default_style_config["rax_cfg"]["ylabel"] = "Step / Initial"

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_shifted_variable(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool = False,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    legend_title: str | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    # create the sum of histograms over all processes
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())

    # setup plotting configs
    plot_config = {}
    colors = {
        "nominal": "black",
        "up": "red",
        "down": "blue",
    }
    for i, shift_id in enumerate(h_sum.axes["shift"]):
        shift_inst = config_inst.get_shift(shift_id)

        h = h_sum[{"shift": hist.loc(shift_id)}]
        # assuming `nominal` always has shift id 0
        ratio_norm = h_sum[{"shift": hist.loc(0)}].values()

        diff = sum(h.values()) / sum(ratio_norm) - 1
        label = shift_inst.label
        if not shift_inst.is_nominal:
            label += " ({0:+.2f}%)".format(diff * 100)

        plot_config[shift_inst.name] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": sum(h.values()) if shape_norm else 1,
                "label": label,
                "color": colors[shift_inst.direction],
            },
            "ratio_kwargs": {
                "norm": ratio_norm,
                "color": colors[shift_inst.direction],
            },
        }
        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    # legend title setting
    if not legend_title:
        if len(hists) == 1:
            # use process label as default if 1 process
            process_inst = list(hists.keys())[0]
            legend_title = process_inst.label
        else:
            # default to `Background` for multiple processes
            legend_title = "Background"

    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )
    default_style_config["rax_cfg"]["ylim"] = (0.25, 1.75)
    default_style_config["rax_cfg"]["ylabel"] = "Ratio"
    default_style_config["legend_cfg"]["title"] = legend_title

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_cutflow(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    style_config: dict | None = None,
    density: bool | None = False,
    #shape_norm: bool = False,
    #relative: bool = False,
    cutflow_type: str | None = None,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    # relative implies shape_norm
    shape_norm = cutflow_type in ("norm", "relative")

    # setup plotting config
    plot_config = prepare_plot_config(
        hists,
        shape_norm=shape_norm,
        hide_errors=hide_errors,
    )

    # switch normalization to normalizing to `initial step` bin
    if shape_norm:
        for key in list(plot_config):
            item = plot_config[key]
            h_key = item["hist"]

            # if iterable (e.g. stack of MC histograms, sum first)
            if isinstance(h_key, Iterable):
                h_key = sum(h for h in h_key)

            if cutflow_type == "relative":
                # binwise normalization to previous bin
                idx_previous = [0] + list(range(len(h_key.axes["step"]) - 1))
                norm = h_key[{"step": idx_previous}].values()
            else:
                # set global normalization to initial yield
                norm = h_key[{"step": "Initial"}].value

            item["kwargs"]["norm"] = norm

    # update xticklabels based on config
    xticklabels = []
    selector_step_labels = config_inst.x("selector_step_labels", {})

    selector_steps = list(hists[list(hists.keys())[0]].axes["step"])
    for step in selector_steps:
        xticklabels.append(selector_step_labels.get(step, step))

    # setup style config
    if not yscale:
        yscale = "linear"

    default_style_config = {
        "ax_cfg": {
            "ylabel": {
                "relative": "Relative efficiency",
                "norm": "Selection efficiency",
                "absolute": "Selection yield",
            }[cutflow_type],
            "xlabel": "Selection step",
            "xticklabels": xticklabels,
            "yscale": yscale,
        },
        "legend_cfg": {
            "loc": "upper right",
        },
        "annotate_cfg": {"text": category_inst.label},
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # ratio plot not used here; set `skip_ratio` to True
    kwargs["skip_ratio"] = True

    fig, (ax,) = plot_all(plot_config, style_config, **kwargs)

    ax.set_xticklabels(xticklabels, rotation=45, ha="right")

    return fig, (ax,)
