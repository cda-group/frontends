import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from skimage import transform
from scipy import ndimage
from skimage import io
import numpy as np
from IPython.display import display, clear_output
import socket
import json
import time

def render(image, heat_map, alpha=0.6, cmap='plasma', axis='on', display=False, save=None, verbose=False):

    height = image.shape[0]
    width = image.shape[1]
    print(height, ' x ', width)
    heat_map_resized = transform.resize(heat_map, (height, width))
    heat_map_resized = heat_map
    max_value = np.max(heat_map_resized)
    min_value = np.min(heat_map_resized)
    if(max_value != 0):
        normalized_heat_map = (heat_map_resized - min_value) / (max_value - min_value)
    else:
        normalized_heat_map = heat_map_resized
    plt.imshow(image)
    plt.imshow(255 * normalized_heat_map, alpha=alpha, cmap=cmap)
    plt.axis(axis)

    if display:
        plt.show(block=False)

    if save is not None:
        if verbose:
            print('save image: ' + save)
        plt.savefig(save, bbox_inches='tight', pad_inches=0)

def transpose(x,y):
    return (835-(y*120),(x*120) + 425)

def nextHeatmap(sock):
    data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
    window = json.loads(data);
    #print("received heatmaps:", window)
    # create heat map
    x = np.zeros((941, 1333))
    for val in window:
        loc = val['f0']
        (tx, ty) = transpose(loc['f0'],loc['f1'])
        x[tx, ty] = val['f1']
    return x

def genHeatmaps(UDP_IP="127.0.0.1", UDP_PORT=5005, image_filename = "mbp-demo.png", display=True, save=None):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    sock.bind((UDP_IP, UDP_PORT))
    image = io.imread(image_filename) # 1333 × 941
    mpl.rcParams['figure.dpi'] = 300

    while True:
        heat_map = ndimage.filters.gaussian_filter(nextHeatmap(sock), sigma=35)
        if display:
            clear_output(wait=True)
        render(image, heat_map, alpha=0.5, axis=True, display=display, save=save)

#    FileName = "testout.png"
#    subprocess.call(['open', FileName])



# create heat map
#x = np.zeros((941, 1333))
#print(x)
#x[720, 470] = 3
#x[629, 470] = 2
#x[823, 470] = 3
#x[700, 650] = 4
#x[700, 710] = 3




