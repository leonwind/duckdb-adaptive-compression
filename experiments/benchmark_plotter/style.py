"""This module defines the basic settings of the plots. Use LaTex for typesetting."""
import seaborn as sns
import matplotlib as mpl


def set_custom_style(cparams=None):
    params = {'axes.linewidth': 2,
              'axes.edgecolor': 'black',
              'xtick.bottom': True,
              'ytick.left': True,
              'grid.linestyle': '--',
              'grid.alpha': 0.1,
              'grid.linewidth': 0.1,
              'axes.facecolor': 'white',
              'grid.color': 'lightgray',
              'axes.spines.left': True,
              'axes.spines.bottom': True,
              'axes.spines.right': True,
              'axes.spines.top': True,
              }

    if cparams is not None:
        for k in cparams:
            params[k] = cparams[k]
    sns.set_style('darkgrid', params)

    #  color_cycle = ['#377eba', '#a10628', '#ff7f00', 'black', '#984ea3', '#999999', '#dede00', '#4daf4a', '#f781bf', '#e41a1c']
    #  mpl.rcParams['axes.prop_cycle'] = mpl.cycler(color=color_cycle)
