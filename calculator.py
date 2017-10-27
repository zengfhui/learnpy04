#!/usr/bin/env python3

import sys
import csv
from multiprocessing import Process,Queue

queue_data = Queue()
queue_result = Queue()


class Args():
	def __init__(self):
		self.args = sys.argv[1:]
	
	def _value_after_option(self,option):
		try:
			index = self.args.index(option) + 1
			return self.args[index]
		except:
			print("Parameter Error")
			exit()
	
	@property	
	def _path_configfile(self):
		return self._value_after_option('-c')
#		return '/home/shiyanlou/test.cfg'

	@property
	def _path_userdatafile(self):
		return self._value_after_option('-d')
#		return '/home/shiyanlou/user.csv'
	@property
	def _path_outputfile(self):
		return self._value_after_option('-o')
#		return '/home/shiyanlou/gongzi.csv'

args = Args()

class Config(object):
	def __init__(self):
		self.config = self._read_configfile()

	def _read_configfile(self):

		configfile = args._path_configfile
		config =dict()
		with open(configfile) as file:
			for line in file.readlines():
				key,value = line.strip().strip().split('=')
				key = key.strip()
				value = value.strip()
				try:
					config[key] = float(value)
				except:
					print(line)
					print('configfile have error')
					exit()
		return config

	def _get_config(self,key):
		try:
			return self.config[key]
		except:
			print(key,'no exist')
			exit()

	@property
	def _JiShuL(self):
		return(self._get_config('JiShuL'))

	@property
	def _JiShuH(self):
		return(self._get_config('JiShuH'))

	@property
	def _rate(self):
		s = sum([
			self._get_config('YangLao'),
			self._get_config('YiLiao'),
			self._get_config('ShiYe'),
			self._get_config('GongShang'),
			self._get_config('ShengYu'),
			self._get_config('GongJiJin'),
			])	
		return s	


config = Config()




class UserData(Process):
#	def __init__(self):
#		self.userdate = self._read_userdatafile()

	def _read_userdatafile(self):
		userdatafile = args._path_userdatafile
		userdata = []
		with open(userdatafile) as file:
			for line in file.readlines():
				name,salary_string = line.strip().split(',')
				try:
					salary = int(salary_string)
				except:
					print(line)
					print("userdatafile have error")
					exit()
				yield (name, salary)
	def run(self):
		for data in self._read_userdatafile():
			queue_data.put(data)


class Calculator(Process):

	def calc_insurance_money(self,salary):
		rate = config._rate
		insurance_money = 0
		if salary < config._JiShuL:
			insurance_money = config._JiShuL * rate
		elif salary > config._JiShuH:
			insurance_money = config._JiShuH * rate
		else:
			insurance_money = salary * rate	
		return insurance_money


	def calc_tax_and_remain(self,salary):
		amount = salary - self.calc_insurance_money(salary) - 3500
		tax = 0
		if amount < 0:
		 	tax = 0
		elif amount < 1500:
		 	tax = amount * 0.03
		elif amount < 4500:
		 	tax = (amount * 0.1) - 105
		elif amount < 9000:
		 	tax = (amount * 0.2) - 555
		elif amount < 35000:
		 	tax = (amount * 0.25) - 1005
		elif amount < 55000:
		 	tax =  (amount * 0.3) - 2755
		elif amount < 80000:
		 	tax = (amount * 0.35) - 5505
		else:
		 	tax = (amount * 0.450) -13505

		remain  = salary - self.calc_insurance_money(salary) - tax
		return '{:.2f}'.format(tax),'{:.2f}'.format(remain)

	def calc_for_all_userdata(self):
		while True:
			try:
				name, salary = queue_data.get(timeout=1)
				data = [name,salary]
			except :
				return

			result = []
			insurance_money = '{:.2f}'.format(self.calc_insurance_money(salary))
			tax , remain = self.calc_tax_and_remain(salary)
			data += [insurance_money,tax,remain]
			result.append(data)
			yield data

	def run(self):
		for data in self.calc_for_all_userdata():
			queue_result.put(data)

class Exporter(Process):

	def run(self):
		with open(args._path_outputfile,'w') as file:
			while True:
				writer = csv.writer(file)
				try:
					result = queue_result.get(timeout=1)
				except :
					return
				writer.writerow(result)



if __name__ == '__main__':
	workers = [ UserData(), Calculator(), Exporter()]
	for p in workers:
		p.run()


'''
			line_count = len(result) - 1
			for s in result:
				n_count = len(s) - 1 
				for i in s:
					file.write(str(i))
					if n_count > 0:
						file.write(',')
						n_count -= 1

				if line_count > 0:
					file.write('\n')
					line_count -= 1

'''











