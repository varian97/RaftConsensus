import psutil

def getCPULoad():
	print(psutil.cpu_percent(interval=0.1))


for i in range(0, 10) :
	getCPULoad()