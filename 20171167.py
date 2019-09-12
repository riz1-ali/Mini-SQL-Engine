import os
import sys
import csv
import sqlparse
from sqlparse.sql import Where
from prettytable import PrettyTable

folder_path = './files/'
keywords = ['select','from','where','distinct','and','or']

def print_data(data):
	table = PrettyTable()
	table.field_names = data[0]
	head = data[0]
	data.pop(0)
	flag = 0
	for row in data:
		if row :
			flag = 1
			table.add_row(row)
	if flag == 0 :
		temp = ["NULL"] * len(head)
		table.add_row(temp)
	print(table)
	# for i in data :
	# 	lim,c = len(i),0
	# 	for j in i :
	# 		c += 1
	# 		if c == lim:
	# 			print(j)
	# 		else :
	# 			print(j,end=",")


def read_file(path) :
	data = []
	with open(folder_path+path+'.csv') as f:
		file_data = csv.reader(f)
		for line in file_data:
			data.append(line)
	return data


def parse_query(args):
	parsed_query = sqlparse.parse(args.strip())[0].tokens
	label_query = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
	select_flag = 0
	from_flag = 0
	for i in label_query:
		if str(i).lower() == "select" :
			select_flag = 1
		if str(i).lower() == "from" :
			from_flag = 1
	if select_flag == 0 :
		sys.exit("Queries using select are only supported.\nPlease use it as you need to choose a table for query.")
	if from_flag == 0 :
		sys.exit("Query doesn't have \'FROM\' in it.\nPlease use it as you need to choose a table for query.")
	return parsed_query

def cross_product(data1,data2,distinct_flag):
	if not data1:
		data = []
		for i in data2 :
			if distinct_flag == 1 :
				if i not in data:
					data.append(i)
			else :
				data.append(i)
		return data
	data = []
	for i in data1 :
		for j in data2 :
			temp = []
			if isinstance(i,int):
				temp.append(i)
				if isinstance(j,int):
					temp.append(j)
				else :
					for k in j :
						temp.append(k)
			else :
				for k in i :
					temp.append(k)
				if isinstance(j,int):
					temp.append(j)
				else :
					for k in j :
						temp.append(k)
			data.append(temp)
	return data

def filter(data,header,order_attributes):
	result = []
	mask = []
	c = 0
	for attribute in header:
		if attribute in order_attributes:
			mask.append(c)
		c += 1
	for row in data :
		temp = []
		for j in range(len(row)) :
			if j in mask :
				temp.append(row[j])
		result.append(temp)
	return result

def operate_where(data,where_list,attribute_maps,order_attributes,parsed_query,from_args,distinct_flag,aggregate):
	crux = data
	where_query = next(token for token in parsed_query if isinstance(token, Where))
	where_label_query = sqlparse.sql.IdentifierList(where_query).get_identifiers()
	if str(where_list[0]).split()[0].lower() != "where":
		sys.exit("Please check the syntax of your query.")
	c = 0
	and_flag = 0
	or_flag = 0
	conditions = []
	for i in where_label_query:
		if str(i).lower() == "where":
			c += 1
		if c > 1 :
			sys.exit("WHERE used more than once.\nPlease check your query once again.")
		if str(i).lower() == "and" :
			and_flag = 1
		if str(i).lower() == "or" :
			or_flag = 1
		if str(i).lower() not in keywords :
			conditions.append(str(i).strip(' ;'))
	if and_flag == 1 and or_flag == 1:
		sys.exit("Use of AND and OR is not supported together.\nUse them one at a time")
	
	temp_conditions = []
	for i in conditions:
		temp_conditions.append(i.replace(' ',''));
	conditions = temp_conditions.copy()
	restore_attributes = order_attributes.copy()
	while '' in conditions : conditions.remove('')
	for i in conditions :
		less_than,greater_than,equal_to,less_than_equal_to,greater_than_equal_to = 0,0,0,0,0
		des = []
		if '<=' in i :
			less_than_equal_to = 1
			des.append(i.split('<='))
		elif '<' in i :
			less_than = 1
			des.append(i.split('<'))
		elif '>=' in i :
			greater_than_equal_to = 1
			des.append(i.split('>='))
		elif '>' in i :
			greater_than = 1
			des.append(i.split('>'))
		elif '=' in i :
			equal_to = 1
			des.append(i.split('='))
		else :
			sys.exit("Invalid Query Syntax.")
		if len(des) > 1 :
			sys.exit("Please check the syntax of your query.\nYou can only use one conditional statement at a time.")
		name = des[0][0]
		if name.find('.') != -1 :
			if name not in restore_attributes :
				restore_attributes.append(name)
		else :
			for table in from_args :
				if name in attribute_maps[table] and table+"."+str(name) not in restore_attributes:
					restore_attributes.append(table+"."+str(name))
		if des[0][1].isnumeric() == False :
			name = des[0][1]
			if name.find('.') != -1 :
				flag = 0
				for table in from_args:
					for attribute in attribute_maps[table] :
						if table +'.'+ attribute == name :
							flag = 1
				if flag == 0 :
					sys.exit("Invalid Query Syntax.")
				if name not in restore_attributes :
					restore_attributes.append(name)
			else :
				flag = 0
				for table in from_args:
					for attribute in attribute_maps[table] :
						if attribute == name :
							flag = 1
				if flag == 0 :
					sys.exit("Invalid Query Syntax.")
				temp_s = []
				for table in from_args :
					if name in attribute_maps[table] and table+"."+str(name) not in restore_attributes:
						temp_s.append(table+"."+str(name))
				if len(temp_s) > 2 : sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
				if len(temp_s) == 0 : sys.exit("Please check the attributes mentioned again properly.")
				restore_attributes.append(temp_s[1])
	restore_attributes.sort()
	table_attributes = {}
	for table in from_args:
		table_attributes[table] = []
	for table in from_args:
		for attribute in restore_attributes:
			if attribute.split('.')[0] == table :
				table_attributes[table].append(attribute)

	file_data = {}
	for table in from_args :
		data = read_file(table)
		file_data[table]=data

	table_data,table_order_att = {},{}
	header = []
	for table in from_args:
		c = 0
		bit_set = []
		table_order_att[table] = []
		for attribute in attribute_maps[table]:
			if table+"."+attribute in table_attributes[table] :
				bit_set.append(c)
				header.append(table+"."+attribute)
				table_order_att[table].append(table+"."+attribute)
			c += 1
		data = []
		for line in file_data[table]:
			row = []
			for j in range(len(line)) :
				if j in bit_set :
					row.append(int(line[j]))
			if distinct_flag == 1:
				if row not in data:
					data.append(row)
			else :
				data.append(row)
		table_data[table] = data

	table_wise_filter = {}
	ans = []
	if and_flag == 0 and or_flag == 0 :
		less_than,greater_than,equal_to,less_than_equal_to,greater_than_equal_to = 0,0,0,0,0
		des = []
		for i in conditions :
			if '<=' in i :
				less_than_equal_to = 1
			elif '<' in i :
				less_than = 1
				des = i.split('<')
				des = i.split('<=')
			elif '>=' in i :
				greater_than_equal_to = 1
				des = i.split('>=')
			elif '>' in i :
				greater_than = 1
				des = i.split('>')
			elif '=' in i :
				equal_to = 1
				des = i.split('=')
			else :
				sys.exit("Invalid Query Syntax.")
		query_att = []
		for x in des :
			name = x
			if name.find('.') == -1 :
				for table in from_args :
					for attribute in attribute_maps[table] :
						if name == attribute :
							query_att.append(table+'.'+attribute)
		query_att.sort()
		condition = des[1]
		if des[1].isdigit() == True :
			des[1] = int(des[1])
			for i in query_att :
				result = []
				query_attribute = str(i)
				table = query_attribute.split('.')[0]
				c = 0
				for j in table_order_att[table]:
					if j == query_attribute :
						break
					c += 1
				if c == len(table_order_att[table]):
					sys.exit("Condition Attribute not written properly.\nPlease check your query once again.")
				c = 0
				fin = 0
				if des[0].find('.') == -1 :
					temp = []
					for table in from_args:
						for attribute in attribute_maps[table] :
							if attribute == des[0] :
								temp.append(table+'.'+attribute)
								fin = c
								mark = 1
							c += 1
					if len(temp) > 2 : sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
					if len(temp) == 0 : sys.exit("Please check the attributes mentioned again properly.")
				else :
					mark = 0
					for table in from_args:
						for attribute in attribute_maps[table] :
							if table+'.'+attribute == des[0] :
								mark = 1
					if mark == 0 :
						sys.exit('Invalid attribute mention.\nPlease check your query once again.')
				total_data = []
				for table in from_args :
					total_data = cross_product(total_data,table_data[table],distinct_flag)
				c = fin
				for row in total_data:
					if less_than :
						if row[c] < des[1] :
							if distinct_flag == 1:
								if row not in result :
									result.append(row)
							else :
								result.append(row)
					if less_than_equal_to :
						if row[c] <= des[1] :
							if distinct_flag == 1:
								if row not in result :
									result.append(row)
							else :
								result.append(row)
					if equal_to :
						if row[c] == des[1] :
							if distinct_flag == 1:
								if row not in result :
									result.append(row)
							else :
								result.append(row)
					if greater_than :
						if row[c] > des[1] :
							if distinct_flag == 1:
								if row not in result :
									result.append(row)
							else :
								result.append(row)
					if greater_than_equal_to :
						if row[c] >= des[1] :
							if distinct_flag == 1:
								if row not in result :
									result.append(row)
							else :
								result.append(row)
				ans = cross_product(ans,result,distinct_flag)
		else :
			cap_att = []
			for i in des :
				mark = 0
				if i.find('.') == -1 :
					for table in from_args :
						for attribute in attribute_maps[table]:
							if attribute == i :
								cap_att.append(table+'.'+attribute)
								mark = 1
				else :
					if i.split('.')[0] not in from_args :
						sys.exit('ERROR: Please mention the tables properly.')
					for attribute in attribute_maps[i.split('.')[0]]:
						if attribute == i.split('.')[1] :
							cap_att.append(i)
							mark = 1
				if mark == 0 :
					sys.exit('Invalid attribute mention.\nPlease check your query once again.')
			if len(cap_att) > 2 :
				sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
			cap_att.sort()
			table_header = {}
			table_data = {}
			x_header = []
			for table in from_args :
				table_data[table] = []
				for line in file_data[table] :
					temp = []
					for i in line :
						temp.append(int(i))
					table_data[table].append(temp)
			total_data = []
			for table in from_args :
				total_data = cross_product(total_data,table_data[table],distinct_flag)
				for attribute in attribute_maps[table]:
					x_header.append(table+'.'+attribute)
			pos = []
			pos_map = {}
			c = 0
			for i in x_header :
				if i in cap_att :
					pos.append(c)
					pos_map[i] = c
				c += 1
			ans = []
			# print(cap_att)
			for row in total_data :
					if less_than :
						if row[pos_map[cap_att[0]]] < row[pos_map[cap_att[1]]] :
							if distinct_flag == 1:
								if row not in ans :	
									ans.append(row)
							else :
								ans.append(row)
					if less_than_equal_to :
						if row[pos_map[cap_att[0]]] <= row[pos_map[cap_att[1]]] :
							if distinct_flag == 1:
								if row not in ans :	
									ans.append(row)
							else :
								ans.append(row)
					if equal_to :
						if cap_att[1] in order_attributes:
							if cap_att[1] not in aggregate :
								if cap_att[1] != cap_att[0] :
									order_attributes.remove(cap_att[1])
						if row[pos_map[cap_att[0]]] == row[pos_map[cap_att[1]]] :
							if distinct_flag == 1:
								if row not in ans :	
									ans.append(row)
							else :
								ans.append(row)
					if greater_than :
						if row[pos_map[cap_att[0]]] > row[pos_map[cap_att[1]]] :
							if distinct_flag == 1:
								if row not in ans :	
									ans.append(row)
							else :
								ans.append(row)
					if greater_than_equal_to :
						if row[pos_map[cap_att[0]]] >= row[pos_map[cap_att[1]]] :
							if distinct_flag == 1:
								if row not in ans :	
									ans.append(row)
							else :
								ans.append(row)
			header = x_header.copy()
	else :
		des = []
		for i in conditions :
			if '<' in i :
				temp = i.split('<')
				temp.insert(1,'<')
				des.append(temp)
			if '<=' in i :
				temp = i.split('<=')
				temp.insert(1,'<=')
				des.append(temp)
			if '=' in i :
				temp = i.split('=')
				temp.insert(1,'=')
				des.append(temp)
			if '>' in i :
				temp = i.split('>')
				temp.insert(1,'>')
				des.append(temp)
			if '>=' in i :
				temp = i.split('>=')
				temp.insert(1,'>=')
				des.append(temp)
		x_header = []
		for table in from_args :
			table_data[table] = []
			for line in file_data[table] :
				temp = []
				for i in line :
					temp.append(int(i))
				table_data[table].append(temp)
		total_data = []
		for table in from_args :
			total_data = cross_product(total_data,table_data[table],distinct_flag)
			for attribute in attribute_maps[table]:
				x_header.append(table+'.'+attribute)
		header = x_header.copy()
		cap_att,pos,pos_map = [],[],{}
		query_att = []
		for cond in des :
			temp = []
			a,b = cond[0],cond[2]
			temp.append(a)
			temp.append(b)
			if a.isnumeric() and b.isnumeric() :
				sys.exit("Cannot have pre-evaluated bool conditions.\nPlease check your query once again.")
			mark = 0
			if a.isnumeric() == True :
				sys.exit("Left Side cannot have constant first.\nPlease check your query once again.")
			else :
				if a.find('.') != -1 :
					for attribute in attribute_maps[a.split('.')[0]] :
						if attribute == a.split('.')[1] :
							cap_att.append(a)
							mark = 1
				else :
					temp = []
					for table in from_args :
						for attribute in attribute_maps[table]:
							if attribute == a :
								temp.append(table+'.'+attribute)
					if len(temp) > 2 or len(temp) == 0:
						sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
					cap_att.append(temp[0])
					mark = 1
			if mark == 0 :
				sys.exit('Invalid attribute mention.\nPlease check your query once again.')
			mark = 0
			if b.isnumeric() == True :
				mark = 1
			else :
				if b.find('.') != -1 :
					for attribute in attribute_maps[b.split('.')[0]] :
						if attribute == b.split('.')[1] :
							cap_att.append(b)
							mark = 1
				else :
					temp = []
					for table in from_args :
						for attribute in attribute_maps[table]:
							if attribute == b :
								temp.append(table+'.'+attribute)
					if len(temp) > 2 or len(temp) == 0:
						sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
					cap_att.append(temp[0])
					mark = 1
			if mark == 0 :
				sys.exit('Invalid attribute mention.\nPlease check your query once again.')
		cap_att.sort()
		c = 0
		for i in x_header :
			if i in cap_att :
				pos.append(c)
				pos_map[i] = c
			c += 1
		set_it = [int(0) for i in range(len(total_data))]
		cond_mark = 0
		for j in des :
			less_than,greater_than,equal_to,less_than_equal_to,greater_than_equal_to = 0,0,0,0,0
			if '<' in j : less_than = 1
			if '<=' in j : less_than_equal_to = 1
			if '=' in j : equal_to = 1
			if '>' in j : greater_than = 1
			if '>=' in j : greater_than_equal_to = 1
			cond_mark += 1
			i = j.copy()
			if i[0].find('.') == -1 :
				temp = []
				for table in from_args :
					for attribute in attribute_maps[table] :
						if attribute == i[0] :
							temp.append(table+'.'+attribute)
				if len(temp) > 2 : sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
				if len(temp) == 0 : sys.exit("Please check the attributes mentioned again properly.")
				i[0] = temp[0]
			if i[2].find('.') == -1 and i[2].isnumeric() == False:
				temp = []
				for table in from_args :
					for attribute in attribute_maps[table] :
						if attribute == i[2] :
							temp.append(table+'.'+attribute)
				if len(temp) > 2 : sys.exit("Ambiguity in query.\nPlease mention the attributes properly.")
				if len(temp) == 0 : sys.exit("Please check the attributes mentioned again properly.")
				i[2] = temp[0]
			c = 0
			if cond_mark == 1:
				for row in total_data :
					if i[2].isnumeric() == False :
						if less_than == 1 :
							if row[pos_map[i[0]]] < row[pos_map[i[2]]] :
								set_it[c] = 1
						if less_than_equal_to == 1 :
							if row[pos_map[i[0]]] <= row[pos_map[i[2]]] :
								set_it[c] = 1
						if equal_to == 1 :
							if i[2] in order_attributes:
								if i[2] not in aggregate :
									if i[2] != i[0] :
										order_attributes.remove(i[2])
							if row[pos_map[i[0]]] == row[pos_map[i[2]]] :
								set_it[c] = 1
						if greater_than == 1 :
							if row[pos_map[i[0]]] > row[pos_map[i[2]]] :
								set_it[c] = 1
						if greater_than_equal_to == 1 :
							if row[pos_map[i[0]]] >= row[pos_map[i[2]]] :
								set_it[c] = 1
					else :
						if less_than == 1 :
							if row[pos_map[i[0]]] < int(i[2]) :
								set_it[c] = 1
						if less_than_equal_to == 1 :
							if row[pos_map[i[0]]] <= int(i[2]) :
								set_it[c] = 1
						if equal_to == 1 :
							if row[pos_map[i[0]]] == int(i[2]) :
								set_it[c] = 1
						if greater_than == 1 :
							if row[pos_map[i[0]]] > int(i[2]) :
								set_it[c] = 1
						if greater_than_equal_to == 1 :
							if row[pos_map[i[0]]] >= int(i[2]) :
								set_it[c] = 1
					c += 1
			else :
				for row in total_data :
					if and_flag == 1 :
						if set_it[c] == 0 :
							c += 1
							continue
					set_it[c] = 0
					if i[2].isnumeric() == False :
						if less_than == 1 :
							if row[pos_map[i[0]]] < row[pos_map[i[2]]] :
								set_it[c] = 1
						if less_than_equal_to == 1 :
							if row[pos_map[i[0]]] <= row[pos_map[i[2]]] :
								set_it[c] = 1
						if equal_to == 1 :
							if i[2] in order_attributes:
								if i[2] not in aggregate :
									order_attributes.remove(i[2])
							if row[pos_map[i[0]]] == row[pos_map[i[2]]] :
								set_it[c] = 1
						if greater_than == 1 :
							if row[pos_map[i[0]]] > row[pos_map[i[2]]] :
								set_it[c] = 1
						if greater_than_equal_to == 1 :
							if row[pos_map[i[0]]] >= row[pos_map[i[2]]] :
								set_it[c] = 1
					else :
						if less_than == 1 :
							if row[pos_map[i[0]]] < int(i[2]) :
								set_it[c] = 1
						if less_than_equal_to == 1 :
							if row[pos_map[i[0]]] <= int(i[2]) :
								set_it[c] = 1
						if equal_to == 1 :
							if row[pos_map[i[0]]] == int(i[2]) :
								set_it[c] = 1
						if greater_than == 1 :
							if row[pos_map[i[0]]] > int(i[2]) :
								set_it[c] = 1
						if greater_than_equal_to == 1 :
							if row[pos_map[i[0]]] >= int(i[2]) :
								set_it[c] = 1
					c += 1
		ans = []
		c = 0
		for row in total_data :
			if set_it[c] == 1 :
				if distinct_flag == 1 :
					if row not in ans :
						ans.append(row)
				else :	
					ans.append(row)
			c += 1
	# print(header)
	ans.insert(0,header)
	# print(order_attributes)
	ans = filter(ans,header,order_attributes)
	return ans

def operate_select(select_args,from_args,attribute_maps,where_list,parsed_query):
	for file in from_args:
		if os.path.exists(folder_path+file+".csv") == False:
			sys.exit("Table "+file+" does not exists.\nPlease check the syntax of your query again.")
	distinct_flag = 0
	temp_args = []
	'''Check if distinct is being used in the query'''
	for args in select_args :
		if args.lower() == "distinct" :
			distinct_flag = 1
			continue
		temp_args.append(args)
	select_args = temp_args.copy()
	aggregate,rep = {},[]
	for args in select_args:
		if '(' in args :
			func = str(args.split('(')[0]).lower()
			name = str((args.split(')')[0]).split('(')[1])
			if name.find('.') != -1 :
				rep.append(name)
				aggregate[name] = func
			else :
				mark = 0
				for table in from_args :
					for attribute in attribute_maps[table] :
						if attribute == name :
							mark = 1
							aggregate[table+"."+name] = func
							rep.append(table+"."+name)
							break
				if mark == 0 :
					sys.exit("Attribute "+str(name)+" is not present in any table/s mentioned in query.\nPlease check your query once again.")

	for args in select_args :
		if args.find('(') != -1 :
			continue
		if args.find('.') != -1 :
			flag = 0
			for table in from_args:
				for attribute in attribute_maps[table]:
					if table+'.'+attribute == args :
						flag = 1
			for table in from_args:
				for attribute in attribute_maps[table]:
					if table+'.'+attribute in aggregate.keys() :
						flag = 0
			if flag == 0:
				sys.exit("Invalid Attribute Usage")
		else :
			flag = 0
			for table in from_args:
				for attribute in attribute_maps[table]:
					if attribute == args :
						flag = 1
			for table in from_args:
				for attribute in attribute_maps[table]:
					if attribute in aggregate.keys() :
						flag = 0
			# print(str(flag)+)
			if flag == 0:
				sys.exit("Invalid Attribute Usage")			
	for i in rep:
		if i in select_args :
			sys.exit("Use Aggregate Functions on Unique Attributes only.\nPlease specify attributes properly and try again.")
		select_args.append(i)
	for i in from_args :
		if os.path.exists(folder_path+i+'.csv') == False :
			sys.exit("Table "+i+" does not exist.\nCreate Table "+i+" and then type the query")
	
	fetch_attributes = {}
	for table in from_args :
		fetch_attributes[table]=[]
	if "*" in select_args :
		for table in from_args :
			for attribute in attribute_maps[table] :
				fetch_attributes[table].append(str(table+"."+attribute))
	else :
		for table in from_args :
			for attribute in select_args :
				if attribute.find('.') != -1:
					if table == attribute.split('.')[0]:
						if attribute.split('.')[1] not in attribute_maps[table] :
							sys.exit("Table "+table+" does not have the attribute "+attribute.split('.')[1])
						fetch_attributes[table].append(attribute)
				else :
					if attribute in attribute_maps[table]:
						fetch_attributes[table].append(str(table+"."+attribute))
	file_data = {}
	for table in from_args :
		data = read_file(table)
		file_data[table]=data
	attribute_data = {}
	for table in from_args:
		if not fetch_attributes[table] :
			continue
		pos = []
		c = 0
		pos_maps = {}
		for i in attribute_maps[table]:
			if str(table+"."+i) in fetch_attributes[table]:
				attribute_data[str(table+"."+i)] = []
				pos_maps[c] = str(table+"."+i)
				pos.append(c)
			c += 1
		for line in file_data[table]:
			for j in range(len(line)) :
				if j in pos :
					if distinct_flag == 1:
						if line[j] not in attribute_data[pos_maps[j]]:
							attribute_data[pos_maps[j]].append(int(line[j]))
					else :
						attribute_data[pos_maps[j]].append(int(line[j]))

	order_attributes = []
	for table in from_args :
		if not fetch_attributes[table] :
			continue
		for attribute in fetch_attributes[table]:
			order_attributes.append(attribute)

	table_row_data = {}
	for table in from_args :
		table_row_data[table] = []
		c = 0
		pos = []
		for i in attribute_maps[table]:
			if str(table+"."+i) in fetch_attributes[table]:
				attribute_data[str(table+"."+i)] = []
				pos_maps[c] = str(table+"."+i)
				pos.append(c)
			c += 1
		for line in file_data[table]:
			temp = []
			for j in range(len(line)) :
				if j in pos :
					temp.append(int(line[j]))
			if distinct_flag == 1:
				if temp not in table_row_data[table] :
					table_row_data[table].append(temp)
			else :
				table_row_data[table].append(temp)
	data = []
	for table in from_args:
		data = cross_product(data,table_row_data[table],distinct_flag)
	data.insert(0,order_attributes)
	if where_list :
		data = operate_where(data,where_list,attribute_maps,order_attributes,parsed_query,from_args,distinct_flag,aggregate)
	if not aggregate :
		if distinct_flag == 1:
			tans = []
			for row in data :
				if row not in tans :
					tans.append(row)
			data = tans.copy()
		print_data(data)
	else :
		manipulate,pos = [],{}
		c = 0
		for i in data[0]:
			pos[i] = c
			manipulate.append([])
			c += 1
		data.pop(0)
		for row in data :
			for i in range(len(row)):
				manipulate[i].append(row[i])
		ans,header,pos_map,c = [],[],{},0
		for i in aggregate :
			pos_map[i] = c
			header.append(i)
			c += 1
		num = [0 for _ in range(c)]
		data = []
		for i in header:
			if aggregate[i] == 'max' :
				if len(manipulate[pos[i]]) > 0:
					data.append(max(manipulate[pos[i]]))
			if aggregate[i] == 'min' :
				if len(manipulate[pos[i]]) > 0:
					data.append(min(manipulate[pos[i]]))
			if aggregate[i] == 'sum':
				if len(manipulate[pos[i]]) > 0:
					data.append(sum(manipulate[pos[i]]))
			if aggregate[i] == 'avg':
				if len(manipulate[pos[i]]) > 0 :
					data.append(sum(manipulate[pos[i]])/len(manipulate[pos[i]]))
				else :
					data.append("NULL")
		temp_header = []

		''' Add names of aggregate functions to Attributes '''
		for head in header :
			temp_h = head
			if head in aggregate :
				func_name = aggregate[head]
				temp_h = func_name+"("+temp_h+")"
			temp_header.append(temp_h)

		header = temp_header
		ans.insert(0,header)
		ans.append(data)
		if distinct_flag == 1:
			tans = []
			for row in data :
				if row not in tans :
					tans.append(row)
			data = tans.copy()		
		print_data(ans)

def process_query(parsed_query,attribute_maps):
	label_query = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
	select_list = []
	from_list = []
	where_list = []
	select_flag = 0
	from_flag = 0
	where_flag = 0
	for i in label_query :
		if str(i).lower() == "select" :
			select_flag += 1		
		if str(i).lower() == "from" :
			from_flag += 1
		if str(i).lower() == "where" :
			where_flag += 1
		if select_flag > 1 :
			sys.exit("SELECT only comes once in an SQL query.\nPlease check your query again.")
		if from_flag > 1 :
			sys.exit("FROM only comes once in an SQL query.\nPlease check your query again.")
		if where_flag > 1 :
			sys.exit("WHERE only comes once in an SQL query.\nPlease check your query again.")
	label_query = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
	select_flag,from_flag,where_flag,distinct_flag = 0,0,0,0
	for i in label_query :
		if str(i).lower() == "select" :
			select_flag = 1
			continue
		if str(i).lower() == "from" :
			select_flag,from_flag = 0,1
			continue
		if str(i).lower() == "where" :
			select_flag,from_flag,where_flag = 0,0,1
		if select_flag == 1 :
			data = str(i).split(',')
			for j in data :
				select_list.append(j.strip('; '))
		if from_flag == 1 :
			data = str(i).split(',')
			for j in data :
				from_list.append(str(j).strip('; '))
		if where_flag == 1 :
			where_list.append(str(i).strip('; '))
	for i in select_list :
		if i.lower() == "distinct" :
			distinct_flag = 1
			if select_list[0].lower() != "distinct" :
				sys.exit("DISTINCT comes before attribute names.\nPlease check your query again.")
	while '' in from_list : from_list.remove('')
	c = 0
	for i in from_list:
		if str(i).lower() == "from": 
			sys.exit("FROM only comes once in an SQL query.\nPlease check your query again.")
		if str(i).split()[0].lower() == "where":
			break
		c += 1
	temp = from_list[c:]
	where_list = temp
	from_list = from_list[:c]
	select_list.sort()
	from_list.sort()
	where_list.sort()
	if len(select_list) == 0 :
		sys.exit("Please mention what to select.")
	if len(from_list) == 0 :
		sys.exit("Please mention tables to select from.")
	if where_flag == 1 and len(where_list)==0 :
		sys.exit("WHERE Clause can't be empty.\nPlease check your query and try again.")
	operate_select(select_list,from_list,attribute_maps,where_list,parsed_query)


def read_metadata():
	attribute_maps = {}
	table_checked = 0
	table_name = ""
	f = open(folder_path+'metadata.txt','r')
	for file_line in f:
		if file_line[len(file_line)-1] == '\n':
			file_line = file_line[:len(file_line)-1]
		if file_line == "<begin_table>":
			table_checked = 1
			continue
		if file_line == "<end_table>":
			table_checked = 0
			continue
		if table_checked == 1 :
			table_checked = 2
			table_name = file_line
			attribute_maps[table_name] = []
			continue
		attribute_maps[table_name].append(file_line)
	return attribute_maps

if len(sys.argv) == 2:
	if len(sys.argv[1]) == 0 :
		sys.exit("Invalid Query")
	parse_query = parse_query(sys.argv[1])
	attribute_maps = read_metadata()
	process_query(parse_query,attribute_maps)
else :
	sys.exit("ERROR: Please type an SQL Query.\nUsage: python3 20171167.py \"<query>\" ")