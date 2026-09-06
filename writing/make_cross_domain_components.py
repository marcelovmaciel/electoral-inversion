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
CABINET_LABEL_POSITIONS = {
    '2016.2': (3.9, 3.25),
    '2017.1': (1.9, .80),
    '2021.3/2022.1': (8.1, -4.35),
    '2023.1': (8.5, .55),
}


def render_cross_domain_components(data: pd.DataFrame, output: Path) -> Path:
    """Draw only distinct cabinets and all exact-connected minimal winners."""
    data = data.loc[data.domain.isin(DOMAINS)].copy()
    if data.duplicated(['domain', 'configuration_id']).any():
        raise ValueError('Duplicate cross-domain configuration.')
    counts = {domain: (len(group), int(group.inversion.sum()))
              for domain, group in data.groupby('domain')}
    if counts != {'cabinet': (22, 4), 'k=0': (20, 6)}:
        raise ValueError(f'Cross-domain configuration/inversion counts changed: {counts}')
    for lhs, rhs in (
        (data.d_C, data.A_C + data.B_C),
        (data.R_C - 1, data.A_C / data.q_C + data.B_C / data.q_C),
        (data.A_pct_quota, 100 * data.A_C / data.q_C),
        (data.B_pct_quota, 100 * data.B_C / data.q_C),
    ):
        if not np.allclose(lhs, rhs, atol=1e-10, rtol=0):
            raise ValueError('Cross-domain decomposition or normalized-axis identity failed.')

    # Rounded design limits retain the approved shared framing without reading
    # the diagnostic k=1 panel or any of its outputs.
    xlim, ylim = (-11.3, 12.1), (-5.7, 5.0)
    if not (data.A_pct_quota.between(*xlim) & data.B_pct_quota.between(*ylim)).all():
        raise ValueError('Cross-domain data exceed the approved figure limits.')
    with plt.rc_context({
        'font.family': 'DejaVu Sans', 'font.size': 9.5,
        'axes.spines.top': False, 'axes.spines.right': False,
        'pdf.fonttype': 42, 'ps.fonttype': 42,
    }):
        fig, axes = plt.subplots(1, 2, figsize=(7.6, 4.5), sharex=True, sharey=True)
        titles = ('A  Observed cabinet configurations',
                  'B  Minimal connected winning coalitions')
        for ax, domain, title in zip(axes, DOMAINS, titles):
            group = data.loc[data.domain == domain]
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
            ax.set_title(title, fontsize=10, loc='left', pad=29)
            ax.text(0, 1.045,
                    f'{len(group)} configurations; {int(group.inversion.sum())} inversions',
                    transform=ax.transAxes, fontsize=9.5, va='bottom')

        def label(ax, row, text, position):
            ax.annotate(text, (row.A_pct_quota, row.B_pct_quota), xytext=position,
                        textcoords='data', ha='center', va='center', fontsize=9.5,
                        zorder=5, arrowprops=dict(arrowstyle='-', color='#69747c',
                                                  lw=.6, shrinkA=2, shrinkB=5))

        for row in data.loc[(data.domain == 'cabinet') & data.inversion].itertuples():
            label(axes[0], row, row.display_label, CABINET_LABEL_POSITIONS[row.display_label])
        ideological_positions = {
            ('MDB', 'UNIÃO'): ('2022 MDB--UNIÃO', (3.3, 4.25)),
            ('PP', 'PL'): ('2022 PP--PL', (7.0, 2.3)),
        }
        for row in data.loc[(data.domain == 'k=0') & (data.election == 2022)
                            & data.inversion].itertuples():
            text, position = ideological_positions[(row.start_party, row.end_party)]
            label(axes[1], row, text, position)

        fig.supxlabel('Within-district contribution (% of coalition quota)', y=.125, fontsize=10)
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
        fig.subplots_adjust(left=.105, right=.99, bottom=.25, top=.80, wspace=.14)
        output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output, facecolor='white', metadata={'CreationDate': None, 'ModDate': None})
        plt.close(fig)
    return output


def save_cross_domain_components(artifact_root: Path, figure_dir: Path) -> Path:
    """Build from audited production outputs and render the manuscript PDF."""
    data = build_cross_domain_components(artifact_root)
    return render_cross_domain_components(data, figure_dir / 'cross_domain_components.pdf')
