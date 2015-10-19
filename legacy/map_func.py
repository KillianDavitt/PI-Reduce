#!usr/bin/python

import threading
import time

def map (names):
	d = {}
	for el in names:
		if d.has_key(el):
			d[el] = d[el]+1
		else:
			d[el] = 1
	return d
		
with open('Fifty_Shades_of_Grey_-_E_L_James.txt', 'r') as f:
	data = f.read()
	words = data.split()
d = map(words)
print(d)