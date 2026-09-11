"""Render the manuscript comparison from the production decomposition module."""
from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator
import numpy as np
import pandas as pd

DECOMPOSITION_DIR = Path(__file__).resolve().parents[1] / 'processing/Processing/decomposition'
sys.path.insert(0, str(DECOMPOSITION_DIR))
from cross_domain_components import build_cross_domain_components  # noqa: E402

YEARS = (2014, 2018, 2022)
# The approved diagnostic election palette.
COLORS = {2014: '#28678b', 2018: '#ba6620', 2022: '#278275'}
DOMAINS = ('cabinet', 'k=0')


def render_cross_domain_components(data: pd.DataFrame, output: Path) -> Path:
    """Draw cabinet periods and all exact-connected minimal winners."""
    validation = data.attrs.get("validation", {})
    unavailable_count = int(validation.get("unidentified_periods", 0))
    unavailable_days = int(validation.get("unidentified_days", 0))
    data = data.loc[data.domain.isin(DOMAINS)].copy()
    if data.duplicated(['domain', 'configuration_id']).any():
        raise ValueError('Duplicate cross-domain configuration.')
    counts = {domain: (len(group), int(group.inversion.sum()))
              for domain, group in data.groupby('domain')}
    if set(data.loc[data.domain == 'k=0', 'ideological_universe']) != {'seat_winning'}:
        raise ValueError('Primary ideological panel requires the seat-winning universe.')
    if not np.allclose(data.d_C, data.A_C + data.B_C, atol=1e-10, rtol=0):
        raise ValueError('Cross-domain additive identity failed.')
    normalized = data.loc[data.q_C.gt(0)]
    for lhs, rhs in (
        (normalized.R_C - 1, normalized.A_C / normalized.q_C + normalized.B_C / normalized.q_C),
        (normalized.A_pct_quota, 100 * normalized.A_C / normalized.q_C),
        (normalized.B_pct_quota, 100 * normalized.B_C / normalized.q_C),
    ):
        if not np.allclose(lhs, rhs, atol=1e-10, rtol=0):
            raise ValueError('Cross-domain decomposition or normalized-axis identity failed.')

    # Rounded design limits retain the approved shared framing without reading
    # the diagnostic k=1 panel or any of its outputs.
    xlim = (min(-11.3, float(data.A_pct_quota.min()) - 1.2), max(12.1, float(data.A_pct_quota.max()) + 1.2))
    ylim = (min(-5.7, float(data.B_pct_quota.min()) - .8), max(5.0, float(data.B_pct_quota.max()) + .8))
    with plt.rc_context({
        'font.family': 'DejaVu Sans', 'font.size': 9.5,
        'axes.spines.top': False, 'axes.spines.right': False,
        'pdf.fonttype': 42, 'ps.fonttype': 42,
    }):
        fig, axes = plt.subplots(1, 2, figsize=(7.6, 4.5), sharex=True, sharey=True)
        titles = ('A  Observed cabinet periods',
                  'B  Minimal connected winning coalitions')
        for ax, domain, title in zip(axes, DOMAINS, titles):
            group = data.loc[data.domain == domain]
            zero_quota = int(group.q_C.eq(0).sum())
            ax.axhline(0, color='#9a9fa4', lw=.65, zorder=0)
            ax.axvline(0, color='#9a9fa4', lw=.65, zorder=0)
            ax.plot(xlim, [-xlim[0], -xlim[1]], color='#67727a', lw=.85,
                    ls=(0, (4, 3)), zorder=1)
            ax.grid(alpha=.10, lw=.5)
            for year in YEARS:
                ordinary = group.loc[(group.election == year) & ~group.inversion]
                ax.scatter(ordinary.A_pct_quota, ordinary.B_pct_quota, s=28,
                           c=COLORS[year], edgecolors='none', alpha=.65, zorder=2)
            for year in YEARS:
                inverted = group.loc[(group.election == year) & group.inversion]
                ax.scatter(inverted.A_pct_quota, inverted.B_pct_quota, s=62,
                           c=COLORS[year], edgecolors='#20252b', linewidths=1.15,
                           alpha=.95, zorder=3)
            ax.set(xlim=xlim, ylim=ylim)
            ax.xaxis.set_major_locator(MultipleLocator(5))
            ax.yaxis.set_major_locator(MultipleLocator(2))
            ax.tick_params(labelsize=9)
            ax.set_title(title, fontsize=10, loc='left', pad=38 if unavailable_count else 29)
            unit = 'periods' if domain == 'cabinet' else 'configurations'
            count_label = f'{len(group)} {unit}; {int(group.inversion.sum())} inversions'
            if zero_quota:
                count_label += f'\n{zero_quota} zero-quota sets (normalized coordinates unavailable)'
            if domain == 'cabinet' and unavailable_count:
                count_label += f'\n{unavailable_count} unidentified intervals ({unavailable_days} days), unplotted'
            ax.text(0, 1.025,
                    count_label,
                    transform=ax.transAxes, fontsize=8.3 if domain == 'cabinet' and unavailable_count else 9.5, va='bottom')

        def label(ax, row, text, position):
            ax.annotate(text, (row.A_pct_quota, row.B_pct_quota), xytext=position,
                        textcoords='data', ha='center', va='center', fontsize=9.5,
                        zorder=5, arrowprops=dict(arrowstyle='-', color='#69747c',
                                                  lw=.6, shrinkA=2, shrinkB=5))

        cabinet_inversions = data.loc[(data.domain == 'cabinet') & data.inversion]
        if cabinet_inversions.empty:
            axes[0].text(.04, .96, 'No identified cabinet inversions', transform=axes[0].transAxes,
                         ha='left', va='top', fontsize=8.5, color='#555555')
        for number, row in enumerate(cabinet_inversions.itertuples()):
            position = (max(xlim[0] + 1.5, min(xlim[1] - 1.5, row.A_pct_quota + (-1.5 if number % 2 else 1.5))),
                        max(ylim[0] + .5, min(ylim[1] - .5, row.B_pct_quota + (.7 if number % 2 else -.7))))
            label(axes[0], row, row.display_label, position)
        focal = data.loc[(data.domain == 'k=0') & data.inversion &
                         (data.is_strongest_inversion | (data.B_C > data.A_C))]
        for number, row in enumerate(focal.itertuples()):
            text = f'{row.election} {row.start_party}-{row.end_party}'
            # Distribute annotations within the shared plot limits; the point
            # identity and text always come from the regenerated registry.
            position = (max(xlim[0] + 3, min(xlim[1] - 3, row.A_pct_quota - 2.4)),
                        max(ylim[0] + .7, min(ylim[1] - .6, row.B_pct_quota + 1.1 + .25 * (number % 2))))
            # The 2014 strongest case sits below a tight cluster of 2018
            # points. Place its annotation in the open lower-right area.
            if row.election == 2014 and row.is_strongest_inversion:
                position = (min(xlim[1] - 3, row.A_pct_quota + 2.5), row.B_pct_quota - 1.7)
            label(axes[1], row, text, position)

        bounded_dates = int(validation.get('bounded_date_periods', 0)) > 0
        fig.supxlabel('Within-district contribution (% of coalition quota)', y=.16 if bounded_dates else .125, fontsize=10)
        if bounded_dates:
            fig.text(.53, .105, 'Cabinet dates include bounded conventions; local date sensitivity is reported separately.',
                     ha='center', fontsize=7)
        fig.supylabel('Between-district contribution (% of coalition quota)', x=.016, fontsize=10)
        handles = [Line2D([], [], ls='', marker='o', mfc=COLORS[year], mec='none',
                          label=str(year), markersize=6) for year in YEARS]
        handles.extend([
            Line2D([], [], ls='', marker='o', mfc='white', mec='#20252b', mew=1.15,
                   label='Inversion (outlined)', markersize=7),
            Line2D([], [], color='#67727a', lw=.85, ls=(0, (4, 3)),
                   label=r'Diagonal: $R_C=1$'),
        ])
        fig.legend(handles=handles, ncol=5, loc='lower center', bbox_to_anchor=(.53, .015),
                   frameon=False, handlelength=1.5, columnspacing=1.1, fontsize=9.5)
        fig.subplots_adjust(left=.105, right=.99, bottom=.29 if bounded_dates else .25, top=.77 if unavailable_count else .80, wspace=.14)
        output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output, facecolor='white', metadata={'CreationDate': None, 'ModDate': None})
        plt.close(fig)
    return output


def save_cross_domain_components(artifact_root: Path, figure_dir: Path) -> Path:
    """Build from audited production outputs and render the manuscript PDF."""
    data = build_cross_domain_components(artifact_root)
    data.to_csv(artifact_root / 'figure_data/cross_domain_components.csv', index=False, float_format='%.17g')
    return render_cross_domain_components(data, figure_dir / 'cross_domain_components.pdf')
