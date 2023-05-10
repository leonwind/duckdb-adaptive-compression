"""This module has some utility functions for plotting with matplotlib"""
from matplotlib import pyplot as plt


def custom_annotation(axis, text, text_position_relative, arrow_position, color, arrow_style='-', fontsize=7,
                      arrow_head='>', arrow_marker_size=3):
    if arrow_head != '>':
        axis.plot(arrow_position[0], arrow_position[1],
                  arrow_head, color='black', markersize=arrow_marker_size)

    if arrow_style.find('o') != -1:
        arrow_style = arrow_style.replace('o', '')
        axis.plot(arrow_position[0], arrow_position[1], 'o', color='black', markersize=3, zorder=100)

    axis.annotate(text,
                  xy=arrow_position,
                  xycoords='data',
                  xytext=text_position_relative, textcoords='axes fraction',
                  arrowprops=dict(arrowstyle=arrow_style,
                                  linewidth=0.7, color=color, mutation_scale=6),
                  horizontalalignment='right',
                  verticalalignment='top',
                  fontsize=fontsize,
                  color=color,
                  zorder=100,
                  )
    text = axis.texts.pop()
    axis.figure.texts.append(text)
    plt.setp(text, multialignment='center')

