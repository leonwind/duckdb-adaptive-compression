"""Define extra colors that can be consistently used for paper plots"""
def blend(color, factor):
    return int(255 - (255 - color) * factor)


def rgb(red, green, blue, factor=1.0):
    """Return color as #rrggbb for the given color values."""
    return f'#{blend(red, factor):02x}{blend(green, factor):02x}{blend(blue, factor):02x}'


colors = {
    'blue': rgb(55, 126, 184),
    'lightblue': rgb(55, 126, 184, 0.5),
    'green': rgb(77, 175, 74),
    'lightgreen': rgb(77, 175, 74, 0.6),
    'orange': rgb(255, 127, 0),
    'lightorange': rgb(255, 127, 0, 0.75),
    'red': rgb(228, 26, 28),
    'lightred': rgb(228, 26, 28, 0.5),
    'black': rgb(0, 0, 0)
}
