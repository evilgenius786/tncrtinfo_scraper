import csv
import json
import os.path
import threading
import time
import traceback
from datetime import datetime

import pandas as pd
import requests
from UliPlot.XLSX import auto_adjust_xlsx_column_width
from bs4 import BeautifulSoup
from dateutil.parser import parse
from flask import Flask, render_template, request, redirect, session

app = Flask(__name__, template_folder='.')

email = ""
password = ""
options = ['Circuit Court', 'General Sessions', 'Clerk and Master']
thread_count = 50
semaphore = threading.Semaphore(thread_count)
lock = threading.Lock()
dump_time = 1
with open('headers.txt') as hfile:
    headers = hfile.read().splitlines()
results = []
scraped = []
test = False
counties = ['greene', 'hawkins', 'jefferson', 'sullivan', 'hamblen', 'washington', 'johnson']
creds = {

}
headers_map = {
    "Subtitle": "dockets",
    "Party Role": "clienttype",
    "Status Date": "uploaddate",
    # "Filings-Filing Date": "filedate",
    "Filing Date": "filedate",
    "Status": "status",
    "County": "counties",
    "Case Judge": "casejudge",
    "Filings-Filing Against": "defendentproper",
    "Party Name": "defendentproper",
    "Filings-Filing For": "plaintiff",
    "Balance Due": "amountofsuit",
    "Party Info-Address Information": "defendentaddress",
    "street": "street",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "Party Info-Phone Information": "phone",
    "Party Info-Email Information": "email",
    "Party Info-Employer Information": "employers",
    "Party Info-Additional Information": "additional_info",
    "Documents-Document Type": "summons",
    "ID": "ID",
    "Plaintiff": "plaintiff",
    "Defendant": "defendentproper",
}
hdrs = []
for hm in headers_map.keys():
    if headers_map[hm] not in hdrs:
        hdrs.append(headers_map[hm])


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')


@app.route('/refreshAll', methods=['GET', 'POST'])
def refreshAll():
    if request.method == 'POST':
        try:
            print(request)
            var = threading.Thread(target=main).start()
            return {"msg": "success"}
        except:
            traceback.print_exc()
            return {"msg": "error"}


@app.route('/refresh', methods=['GET', 'POST'])
def refresh():
    if request.method == 'POST':
        try:
            print(request.form)
            county = request.form.get("county")
            uid = request.form.get("id")
            print(f"Refreshing {uid} {county}")
            var = threading.Thread(target=scrape, args=(county, requests.Session(), {"ID": uid},)).start()
            return {"msg": "success"}
        except:
            traceback.print_exc()
            return {"msg": "error"}


@app.route('/remove', methods=['GET', 'POST'])
def remove():
    if request.method == 'POST':
        try:
            print(request.form)
            county = request.form.get("county")
            uid = request.form.get("id")
            print(f"Removing {uid} {county}")
            var = threading.Thread(target=deleteRow, args=(county, uid,)).start()
            return {"msg": "success"}
        except:
            traceback.print_exc()
            return {"msg": "error"}


def generateTable():
    th = ''
    trs = ""
    rows = []
    fields = []
    for file in os.listdir():
        if file.endswith('.csv') and "Error" not in file and "all" not in file:
            print(f"Working on {file}")
            with open(file) as wfile:
                cfile = csv.DictReader(wfile)
                fields = cfile.fieldnames
                if th == "":
                    th = '<th><input id="allcb" name="allcb" type="checkbox"/></th>'
                    for i in range(len(fields)):
                        th += f'<th onclick="onColumnHeaderClicked(event)"><b>{fields[i]}</b></th>\n'
                        # break
                    # th+=f'<th onclick="sortTable({len(fields)+1})"><b>County</b></th>'
                    # print(th)
                    th += '<th></th>'
                for row in cfile:
                    rows.append(row)
                    tr = '<tr><td><input type="checkbox"/></td>'
                    for field in fields:
                        tr += f'<td>{row[field]}</td>'
                    tr += f'<td><button onclick="remove_row(\'{row["counties"]}\',\'{row["ID"]}\')">Delete</button>' \
                          f'<button onclick="refresh(\'{row["counties"]}\',\'{row["ID"]}\')">Refresh</button></td></tr>\n'
                    trs += tr
                # print(trs)
            # break
    with open('all.csv', 'w', newline='') as outfile:
        x = csv.DictWriter(outfile, fieldnames=fields, extrasaction='ignore')
        x.writeheader()
        x.writerows(rows)
    with open('table.html') as tfile:
        table_html = tfile.read()
    with open("index.html", 'w') as tfile:
        tfile.write(table_html.replace('<header></header>', th).replace('<rows></rows>', trs))
        # break


def deleteRow(uid, county):
    pprint(f"deleting row {county} {uid}")
    pass


def scrape(county, s, row, tries=3):
    with semaphore:
        global results
        _id = row['ID']
        params = (('id', _id),)
        row["County"] = county
        pprint(f"{county} Working on {_id}")
        url = f'https://{county}.tncrtinfo.com/cvCaseForm.aspx'
        gsoup = getSoup(s, url, params=params)
        try:
            pprint(f"{county} Working on tab#{0} {_id} {gsoup.find('li', {'class': 'tabactive'}).text}")
            for tr in gsoup.find('table', {"class": 'tblgen'}).find_all('tr'):
                if tr.find('td', {'class': "label"}) is not None:
                    labels = tr.find_all('td', {"class": "label"})
                    fields = tr.find_all('td', {"class": "field"})
                    for label, field in zip(labels, fields):
                        row[label.text.replace(":", "").strip()] = field.text.replace("Make a Payment", "").strip()
                else:
                    for td in tr.find_all('td', {"class": True}):
                        row[" ".join(td['class']).strip().title().replace("gen", "").replace(":", "")] = td.text.strip()
            tabcount = len(gsoup.find('ul', {"id": "cphContent_cphTabbedBar_ultab"}).find_all('li'))
            data = getDataForm(gsoup)
            if 'Bottomtitle' in row.keys() and row['Bottomtitle'] != '' and row['Bottomtitle'].endswith(', Plaintiff'):
                row['Plaintiff'] = row['Bottomtitle'].replace(', Plaintiff', "")
            if 'Bottomtitle' in row.keys() and row['Bottomtitle'] != '' and row['Bottomtitle'].endswith(', Defendant'):
                row['Defendant'] = row['Bottomtitle'].replace(', Defendant', "")
            for i in range(1, tabcount):
                data['__EVENTARGUMENT'] = f"{i}"
                psoup = postSoup(s, url, params, data)
                active = ""
                try:
                    active = psoup.find('li', {'class': 'tabactive'}).text
                except:
                    active = ""
                pprint(f"{county} Working on tab#{i} {_id} {active}")
                data = getDataForm(psoup)
                row.update(getRow(psoup, row))
            keys = [k for k in row.keys()]
            for key in keys:
                if key.endswith("State"):
                    row["State"] = row[key]
                    del row[key]
                elif key not in headers:
                    headers.append(key)
                    pprint(f"{county} New header {key}")
                    with open('headers.txt', 'w') as hfile:
                        hfile.write("\n".join(headers))
            pprint(f"{county} {json.dumps(row, indent=4)}")
            with open(f"./{county}/{_id}.json", 'w', encoding='utf8') as jfile:
                json.dump(row, jfile, indent=4)
            newrow = {}
            for key in row.keys():
                if key in headers_map.keys():
                    data = row[key]
                    if "amount" in headers_map[key]:
                        try:
                            data = float(data.replace(",", "").replace("$", ""))
                        except:
                            traceback.print_exc()
                    elif "date" in headers_map[key]:
                        try:
                            data = f'{parse(data).strftime("%Y-%m-%d")}'
                        except:
                            traceback.print_exc()
                    newrow[headers_map[key]] = data
            newrow['summons'] = ""
            try:
                newrow['summons'] += f"{row['Documents-Document Type']}"
            except:
                pass
            try:
                newrow['summons'] += f" {row['Documents-Status']}"
                newrow['summons'] = newrow['summons'].strip()
            except:
                pass
            if "defendentaddress" in newrow.keys() and len(newrow['defendentaddress'].split()) > 3 and "address" not in \
                    newrow['defendentaddress']:
                newrow.update(getAddress(newrow))
            pprint(f"{county} {json.dumps(newrow, indent=4)}")
            if newrow['plaintiff'] == "" and tries > 0:
                login(county, s)
                return scrape(county, s, row, tries - 1)
            else:
                append(newrow, county)
        except:
            if tries == 0:
                pprint(f"{county} Error ID {_id}")
                with open("Error.csv", 'a') as efile:
                    csv.writer(efile).writerow([_id, county])
                traceback.print_exc()
                pprint(f"{county} {json.dumps(data, indent=4)}")
                pprint(f"{county} {psoup}")
            else:
                return scrape(county, s, row, tries - 1)


def getAddress(line):
    addr = line['defendentaddress'].replace("\n", ', ')
    addr = addr.replace(",,", ",")
    s = addr.split()
    # print(s[-2])
    # print(s[-3])
    if s[-2] in s[-3]:
        addr = addr.replace(f"{s[-3]}", "")
        # print("Replacing")
    return {
        'zip': addr.split()[-1].strip() if addr.split()[-1].strip().isnumeric() else "",
        'state': addr.split()[-2].strip() if addr.split()[-1].strip().isnumeric() else addr.split()[-1].strip(),
        'city': addr.split(",")[-2].strip() if len(addr.split(",")) > 2 else "",
        'street': ",".join(addr.split(",")[0:-2]).strip() if len(addr.split(",")) > 2 else addr.split(",")[-2].strip(),
        "defendentaddress": addr
    }


def login(county, s):
    url = f"https://{county}.tncrtinfo.com/Login.aspx"
    s.headers = {'user-agent': 'Mozilla/5.0'}
    pprint(f"{county} Logging in with {email if county not in creds.keys() else creds[county]['username']}...")
    s.cookies.clear()
    psoup = BeautifulSoup(s.post(url, data=getLoginData(BeautifulSoup(s.get(url).content, 'lxml'), county)).content,
                          'lxml')
    s.cookies.pop('pubinqcrt')
    s.cookies['pubinqcrt'] = f"selcrt=" \
                             f"{psoup.find('select', {'id': 'ddlCourt'}).find('option', string=options[1])['value']}"
    return psoup


def start(county):
    if not os.path.isdir(county):
        os.mkdir(county)
    if not os.path.isfile(f"{county}.csv"):
        with open(f"{county}.csv", 'w', newline='', encoding='utf8', errors='ignore') as sfile:
            csv.DictWriter(sfile, fieldnames=hdrs).writeheader()
    with open(f"{county}.csv", newline='', encoding='utf8', errors='ignore') as sfile:
        for line in csv.DictReader(sfile, fieldnames=hdrs):
            scraped.append(line['ID'])
    s = requests.Session()
    psoup = login(county, s)
    year = "2022"
    # if test:
    #     with open('ids.txt') as ifile:
    #         ids = ifile.read().splitlines()
    #     threads = []
    #     for _id in ids:
    #         if _id not in scraped:
    #             thread = threading.Thread(target=scrape, args=(county, s, {"ID": _id}))
    #             thread.start()
    #             # thread.join()
    #             # input("")
    #             time.sleep(0.1)
    #     print("Spawning done. waiting for them to finish..")
    #     for thread in threads:
    #         thread.join()
    #     time.sleep(1)
    #     input("Done")
    try:
        pprint(f'{county} {psoup.find("span", {"id": "logviewhead_lbllogname"}).text}')
        params = (('search', 'number'),)
        url = f'https://{county}.tncrtinfo.com/cvCaseList.aspx'
        psoup = postSoup(s, url, params=params, data=getDataList(getSoup(s, url, params=params), year))
        try:
            threads = spawnRows(getRows(psoup), s, county)
            pprint(f'{county} {getTotal(psoup)}')
            while True:
                try:
                    mx = int(getTotal(psoup).split()[-1])
                    break
                except:
                    psoup = postSoup(s, url, params=params, data=getDataList(getSoup(s, url, params=params), year))
                    pprint("Retrying...")
            for i in range(1, mx):
                psoup = postSoup(s, url, params=params, data=getNextData(psoup, i, mx, year))
                try:
                    pprint(f'{county} {getTotal(psoup)}')
                except:
                    traceback.print_exc()
                threads.extend(spawnRows(getRows(psoup), s, county))
            for thread in threads:
                thread.join()
            pprint(f"{county} Scraping finished!")
            convert(f"{county}.csv")
        except:
            traceback.print_exc()
            pprint(f'{county} {psoup}')
    except:
        traceback.print_exc()
        pprint(f'{county} {psoup}')
        convert(f"{county}.csv")


def fetchAll():
    logo()
    if test:
        start("washington")
        return
    threads = []
    for county in counties:
        thread = threading.Thread(target=start, args=(county,))
        thread.start()
        threads.append(thread)
        time.sleep(0.1)
    for thread in threads:
        thread.join()
    generateTable()


def getRow(soup, row=None):
    if row is None:
        row = {}
    tabactive = soup.find("li", {"class": "tabactive"}).text
    if "searchList" in str(soup):
        table = soup.find('table', {"class": "searchList"})
        ths = table.find('tr', {"class": "searchListHeader"}).find_all('th')
        if tabactive == "Additional Parties":
            trs = table.find_all('tr')[1:]
            for tr in trs:
                tds = tr.find_all('td')
                row[tds[1].text] = tds[0].text
        else:
            trs = [table.find_all('tr')[1]]
            for tr in trs:
                tds = tr.find_all('td')
                if len(table.find_all('tr')) == 2:
                    for th, td in zip(ths, tds):
                        if len(th.text.strip()) > 1 and len(td.text.strip()) > 1:
                            row[f'{tabactive}-{th.text.replace(":", "").strip()}'] = td.text.strip()
    elif "tblcontact" in str(soup):
        for br in soup.find_all("br"):
            br.replace_with(", ")
        table = soup.find('table', {"class": "tblcontact"})
        ths = table.find_all('td', {'class': 'contactheader'})
        tds = table.find_all('td', {'class': 'contactdetail'})
        for th, td in zip(ths, tds):
            row[f'{tabactive}-{th.text.replace(":", "").strip()}'] = td.text.replace("Home", "").strip().replace(
                "\u00a0", " ")
    else:
        pprint("table none")
    return row


def pprint(msg):
    m = f"{datetime.now()}".split(".")[0] + " | " + msg
    print(m)


def getRows(soup):
    rows = []
    table = soup.find('table', {"class": "searchList"})
    if table is None:
        return []
    slh = table.find('tr', {"class": "searchListHeader"})
    if slh is None:
        return []
    ths = slh.find_all('th')
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) > 0:
            row = {"ID": tds[1].find("a")['href'].split("=")[-1]}
            for th, td in zip(ths, tds):
                if len(th.text.strip()) > 1 and len(td.text.strip()) > 1:
                    row[th.text.strip()] = td.text.strip()
            rows.append(row)
    return rows


def dump(county):
    global results
    while True:
        time.sleep(dump_time)
        if len(results) > 0:
            with lock:
                pprint(f"Dumping to {county}.csv")
                try:
                    with open(f"{county}.csv", 'a', newline='', encoding='utf8', errors='ignore') as sfile:
                        csv.DictWriter(sfile, fieldnames=hdrs).writerows(results)
                    results = []
                except:
                    pprint("Error while dumping")
                    traceback.print_exc()


def append(row, county):
    with lock:
        try:
            with open(f"{county}.csv", 'a', newline='', encoding='utf8', errors='ignore') as sfile:
                csv.DictWriter(sfile, fieldnames=hdrs).writerow(row)
        except:
            pprint("Error while appending")
            traceback.print_exc()


def spawnRows(rows, s, county):
    threads = []
    for row in rows:
        if row['ID'] not in scraped:
            if row['Party Role'] == "Defendant":
                thread = threading.Thread(target=scrape, args=(county, s, row))
                thread.start()
                # thread.join()
                threads.append(thread)
                time.sleep(0.1)
            else:
                pprint(f"Not required party role {row['Party Role']}")
        else:
            pprint(f"Already scraped {row['ID']}")
    return threads


def getTotal(soup):
    try:
        return soup.find('span', {'id': 'cphContent_cphContentPaging_lblpagenum'}).text
    except:
        return ""


def getData(gsoup, data):
    ddl = "ctl00$ctl00$ddlCourt"
    data[ddl] = gsoup.find('select', {"id": 'ddlCourt'}).find("option", string=options[1])['value']
    data[ddl] = gsoup.find('select', {"id": 'ddlCourt'}).find("option", {"selected": "selected"})['value']
    # for div in gsoup.find_all('div', {'class': "aspNetHidden"}):
    for _input in gsoup.find_all('input', {'type': "hidden", 'id': True, "value": True, "name": True}):
        if ("__EVENTARGUMENT" != _input['id'] or "__EVENTARGUMENT" not in data.keys()) and (
                "__EVENTTARGET" != _input['id'] or "__EVENTTARGET" not in data.keys()):
            data[_input['id']] = _input['value']
    return data


def getNextData(soup, pg, mx, year):
    data = {
        'ctl00$ctl00$cphContent$cphSelectionCriteria$txtCaseNumber': '',
        'ctl00$ctl00$cphContent$cphSelectionCriteria$txtCaseYear': year,
        'ctl00$ctl00$cphContent$cphContentPaging$hfpg': pg,
        'ctl00$ctl00$cphContent$cphContentPaging$hfmx': mx,
        'ctl00$ctl00$cphContent$cphContentPaging$nextpage': 'Next >'
    }
    return getData(soup, data)


def getDataForm(soup):
    data = {
        'ctl00$ctl00$cphContent$scrtab': 'ctl00$ctl00$cphContent$upnltabhead|ctl00$ctl00$cphContent$cphTabbedBar$ultab',
        '__EVENTTARGET': 'ctl00$ctl00$cphContent$cphTabbedBar$ultab',
    }
    return getData(soup, data)


def getDataList(soup, year):
    data = {
        'ctl00$ctl00$cphContent$cphSelectionCriteria$txtCaseNumber': '',
        'ctl00$ctl00$cphContent$cphSelectionCriteria$txtCaseYear': year,
        'ctl00$ctl00$cphContent$cphSelectionCriteria$cmdFindNow': 'Find Now'
    }
    return getData(soup, data)


def getLoginData(soup, county):
    data = {
        'ctl00$ctl00$cphContent$cphFormDetail$logmain$UserName':
            email if county not in creds.keys() else creds[county]['username'],
        'ctl00$ctl00$cphContent$cphFormDetail$logmain$Password':
            password if county not in creds.keys() else creds[county]['password'],
        'ctl00$ctl00$cphContent$cphFormDetail$logmain$LoginButton': 'Log In'
    }
    return getData(soup, data)


def convert(file):
    df = pd.read_csv(file)
    with pd.ExcelWriter(file.replace(".csv", ".xlsx")) as writer:
        df.to_excel(writer, sheet_name="MySheet", index=False)
        auto_adjust_xlsx_column_width(df, writer, sheet_name="MySheet", margin=0, index=False)


def getSoup(s, url, params=None):
    try:
        content = s.get(url, params=params).content
        if "Welcome" in str(content):
            return BeautifulSoup(content, 'lxml')
        else:
            pprint("Relogging in...")
            login(url.split(".")[0].replace("https://", ""), s)
            return getSoup(s, url, params)
    except:
        time.sleep(1)
        traceback.print_exc()
        return getSoup(s, url, params)


def postSoup(s, url, params=None, data=None):
    try:
        return BeautifulSoup(s.post(url, params=params, data=data).content, 'lxml')
    except:
        return postSoup(s, url, params, data)


def logo():
    print(fr"""
          __                           __   .__          _____       
        _/  |_  ____    ____ _______ _/  |_ |__|  ____ _/ ____\____  
        \   __\/    \ _/ ___\\_  __ \\   __\|  | /    \\   __\/  _ \ 
         |  | |   |  \\  \___ |  | \/ |  |  |  ||   |  \|  | (  <_> )
         |__| |___|  / \___  >|__|    |__|  |__||___|  /|__|  \____/ 
                   \/      \/                        \/              
==============================================================================
            TennesseePublic Court Records System scraper by:
                https://github.com/evilgenius786
==============================================================================
[+] Multithreaded (thread count: {thread_count})
[+] Scalable
[+] Efficient
[+] Dynamic headers
[+] JSON/CSV/XLSX output
______________________________________________________________________________
""")


def main():
    app.run(host='0.0.0.0', port=18942)


if __name__ == '__main__':
    generateTable()
