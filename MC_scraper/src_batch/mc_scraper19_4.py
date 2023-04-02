import requests
from bs4 import BeautifulSoup
import copy
import re
import os
import json
from multiprocessing import Process

# Libraries required to limit the time taken by a request
import signal
from contextlib import contextmanager

baseurl		= "http://www.moneycontrol.com"
base_dir	= "../output"
company_dir	= base_dir+'/Companies'
category_Company_dir = base_dir+'/Category-Companies'
company_sector = {}

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
	def signal_handler(signum, frame):
		raise TimeoutException
	signal.signal(signal.SIGALRM, signal_handler)
	signal.alarm(seconds)
	try:
		yield
	finally:
		signal.alarm(0)


def ckdir(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)
	return


def get_response(aurl):
	hdr				= {'User-Agent':'Mozilla/5.0'}

	while True:
		try: 
			# Waiting 60 seconds to recieve a responser object
			with time_limit(60):
				content 				= requests.get(aurl,headers=hdr).content
			break
		except Exception:
			print("Error opening url!!")
			continue

	return content

# Procedure to return a parseable BeautifulSoup object of a given url
def get_soup(aurl):
	response 		= get_response(aurl)
	soup 			= BeautifulSoup(response,'html.parser')

	return soup


def get_categories(aurl):
	soup	= get_soup(aurl)
	links = {}
	tables	= soup.find('div',{'class':'lftmenu'})
	print(tables)
	categories = tables.find_all('li')
	for category in categories:
		category_name = category.get_text()
		if category.find('a',{'class':'act'}):
			links[category_name] = aurl
		else:
			links[category_name] = baseurl + category.find('a')['href']
	return links



def get_Data(aurl,aname,fname):

	acc = ""
	for char in aname:
		if(char==' ' or char=='.'):
			char = "_"
		acc = acc+char

	try:
		soup	= get_soup(aurl)
		og_table	= soup.find('div',{'class':'table-responsive financial-table'})
	except AttributeError:
		return

	if(og_table is None):
		print("Error:Table Class")
		return

	table	= og_table.find('table',{'class':'mctable1'})

	if(table is None):
		print("Error:Table")
		return

	rows = table.find_all('tr')

	if(rows is None):
		print("Error:Rows")
		return

	final_rows = ""

	for r in rows:
		td = r.find_all('td')
		row = [i.text for i in td]
		#final_rows = final_rows+'['

		lenrow = len(row)

		for c in range(0,lenrow):
			final_rows = final_rows+'"'
			final_rows = final_rows+row[c]
			final_rows = final_rows+'"'
			#if(c < lenrow-1):
			final_rows = final_rows+','

		final_rows = final_rows+'\n' #'],'
		#print(row)

	ckdir(company_dir+'/'+acc)

	with open(company_dir+'/'+acc+'/'+fname,'w') as outfile:
		outfile.write(final_rows)

	

	return


def get_PL_Data(aurl,aname):
	print("   P&L")
	get_Data(aurl,aname,aname+"-PL.csv")
	return


def get_BS_Data(aurl,aname):
	print("   Balance Sheet")
	get_Data(aurl,aname,aname+"-BS.csv")
	return

def get_results(aurl,aname,num):
	if(num==1):
		p_str = "   Quarterly Results"
		f_str = "_quarterly_results"
	if(num==2):
		p_str = "   Half Yearly Results"
		f_str = "_half-yearly_results"
	if(num==3):
		p_str = "   Nine-Monthly Results"
		f_str = "_nine-monthly_results"
	if(num==4):
		p_str = "   Yearly Results"
		f_str = "_annual_results"
	if(num==5):
		p_str = "   Cash Flow"
		f_str = "_cash-flow"
	if(num==6):
		p_str = "   Ratios"
		f_str = "_ratios"
	print(p_str)
	get_Data(aurl,aname,aname+f_str+".csv")

	return
		

def get_sector(asoup):

	sector = None

	try:
		details = asoup.find('div',{'class':'FL gry10'})
		headers = details.get_text().split('|')
	except AttributeError:
		return sector

	for header in headers:
		if "SECTOR" in header:
			# print(header.split(':')[1].strip())
			sector = header.split(':')[1].strip()
			break

	return sector

def get_Company_Data(aurl,aname):
	soup	= get_soup(aurl)
	temp 	= soup.find("div", {'class':'quick_links clearfix'})

	try:
		links		= temp.find_all(['li'])

	except AttributeError:
		print("Data on '"+aname + "' doesn't exist anymore.")
		return

	for i in range(0,len(links)):
		field_text = links[i].get_text()
		#print(field_text)

		#print(links[i].find_all('a'))
		field = [a.get('href') for a in links[i].find_all('a', href=True)]
		required_link = field[0]

		if field_text == "Profit & Loss":
			get_PL_Data(required_link,aname)

		if field_text == "Balance Sheet":
			get_BS_Data(required_link,aname)
		
		if field_text == "Quarterly Results":
			get_results(required_link,aname,1)
		
		if field_text == "Half Yearly Results":
			get_results(required_link,aname,2)
		
		if field_text == "Nine Months Results":
			get_results(required_link,aname,3)
		
		if field_text == "Yearly Results":
			get_results(required_link,aname,4)
		
		if field_text == "Cash Flows":
			get_results(required_link,aname,5)
		
		if field_text == "Ratios":
			get_results(required_link,aname,6)
		

	company_sector["companies"][aname] = get_sector(soup)

	with open(base_dir+"/company-sector.json",'w') as outfile:
		json.dump(company_sector,outfile)
	return


def get_list(aurl,category):
	details	= []
	soup	= get_soup(aurl)
	filters	= soup.find_all('div',{'class':'MT10'})
	table	= filters[3].find_all('div',{'class':'FL'})[2]
	rows 	= table.find_all('tr')
	headers= rows[0].find_all('th')
	labels	= {}
	for i in range(0,len(headers)):
		labels[i] = headers[i].get_text()

	for row in rows[1:]:
		company = {}
		fields = row.find_all('td')
		for i in range(0,len(headers)):
			company[labels[i]] = fields[i].get_text()
		company['link'] = baseurl + fields[0].find('a')['href']
		get_Company_Data(company['link'],company['Company Name'])
		details.append(company)

	with open(category_Company_dir+'/'+category+'.json','w') as outfile:
		json.dump({'Company_details':details},outfile)


def get_sector_data(aurl):
	categories = get_categories(aurl)

    #with open(base_dir+'/categories.json','w') as outfile:
    #    json.dump(categories,outfile)

	with open(base_dir+"/categories.json",'r') as infile:
	 	categories = json.load(infile)

	category = "Utilities"

	category_url = categories[category]

	print("Accessing companies. Category : "+category)

	company_list	= get_list(category_url,category)


def get_alpha_quotes(aurl):
	soup = get_soup(aurl)

	print(aurl)

	list = soup.find('table',{'class':'pcq_tbl MT10'})

	companies = list.find_all('a')

	p_iter = 0
	pos = 0
	p_limit = 10
	proc = []

	for company in companies[0:]:
		global iter_comp
		global ind_comp
		iter_comp = iter_comp+1
        
		if( (iter_comp%4)!=ind_comp ):
			continue

		if company.get_text() != '':
			print(company.get_text()+" : "+company['href'])
			p = Process(target=get_Company_Data(company['href'],company.get_text()) )
			p.start()
			proc.append(p)
			p_iter = p_iter+1

			if(p_iter == p_limit):
				for p in proc:
					p.join()
				p_iter = 0
				proc = []
	
	for p in proc:
		p.join()




def get_all_quotes_data(aurl):
	soup = get_soup(aurl)
	list = soup.find('div',{'class':'MT2 PA10 brdb4px alph_pagn'})

	links= list.find_all('a')
	global iter_comp

	for link in links[19:20]:
		# print(link.get_text()+" : "+baseurl+link['href'])
		print("Accessing list for : "+link.get_text())
		iter_comp = 0
		get_alpha_quotes(baseurl+link['href'])

if __name__ == '__main__':
	sector_url		= 'http://www.moneycontrol.com/india/stockmarket/sector-classification/marketstatistics/nse/automotive.html'
	quote_list_url 	= 'http://www.moneycontrol.com/india/stockpricequote'

	url 			= quote_list_url
    

	print("Initializing")
	ckdir(base_dir)
	ckdir(company_dir)
	ckdir(category_Company_dir)

	try:
		with open(base_dir+"/company-sector.json",'r') as infile:
			company_sector = json.load(infile)
	except FileNotFoundError:
		company_sector = {"companies":{}}

	# print(company_sector)

	# get_sector_data(url)
	global iter_link
	global iter_comp
	global ind_comp
	iter_link = 0
	iter_comp = 0
	ind_comp = 0
	get_all_quotes_data(url)