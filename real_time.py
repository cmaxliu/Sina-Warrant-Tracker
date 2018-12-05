'''
to fetch real time data
1. real time stock data from Sina api
2. index and future data from aastock
'''

from lxml import html
import requests
import pandas as pd
from datetime import datetime, date

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
column_name = ['name_eng', 'name_chi', 'today_open', 'last_close', 'today_high', 'today_low', 'price', '?1', '?2',
               'bid', 'ask', 'volumn', 'amount', '买二量/股', '买二价', '买三量/股', '买三价', '买四量/股', '买四价', '买五量/股', '买五价',
               '卖一量/股', '卖一价', '卖二量/股', '卖二价', '卖三量/股', '卖三价', '卖四量/股', '卖四价', '卖五量/股', '卖五价', '日期', '时间']


def real_time_future(index):
    rt_fut_price = []
    fut_code = {"HSI": 200000, "HSCEI": 200200}
    for i in range(2):
        url = "http://www.aastocks.com/en/stocks/market/bmpfutures.aspx?future=" + str(fut_code[index] + i)
        page = requests.get(url, headers=headers)
        tree = html.fromstring(page.content)

        expiry_month = int(tree.xpath("//*[@id='ssMain']/div[4]/div[5]/table/tr[6]/td[1]/div[2]//text()").pop()[5:7])
        price = tree.xpath("//*[@id='ssMain']/div[4]/div[5]/table/tr[1]/td[1]/div[4]//text()").pop().strip().replace(
            ",", "")
        if price != 'N/A':
            rt_fut_price.append(int(price))
        else:
            rt_fut_price.append(None)
    return rt_fut_price


# print(real_time_future())

# convert code into the url
def gen_url(input):
    if isinstance(input, list):
        return "http://hq.sinajs.cn/list=" + ",".join(
            ["rt_hk{}".format(str(code).zfill(5)) if isinstance(code, int) else "rt_hk{}".format(code) for code in
             input])
    elif isinstance(input, int):
        return "http://hq.sinajs.cn/list=rt_hk" + str(input).zfill(5)
    else:
        return "http://hq.sinajs.cn/list=rt_hk" + input


# extract data from the url
def get_real_time(codes):
    # to get data from url
    url = gen_url(codes)
    page = requests.get(url, headers=headers)
    page_content = page.content.decode("GBK")

    # to separate data entries
    data_list = page_content.strip().split(";")

    # to delete empty entry
    data_list = [item for item in data_list if item != ""]

    # to parse each entry
    data_output = {}
    for entry in data_list:
        equal_sign_index = entry.index('="')
        code = entry[equal_sign_index - 5:equal_sign_index]
        # convert the figurative code into int format
        try:
            code = int(code)
        except:
            pass
        result = entry[equal_sign_index + 2:-3].split(",")
        result[2:13] = list(map(float, result[2:13]))
        data_output[code] = result[:13]
    return data_output


def real_time(code):
    url = gen_url(code)
    return get_url(url)


def real_time_list(code_list):
    rtdata = {}
    for code in code_list:
        rtdata[code] = real_time(code)
    return rtdata

#import the basic data of the instruments
def get_details(file):
    df = pd.read_csv(file, sep=",", names=["code", "ex_price", "lot_size", "ex_year", "ex_month", "ex_day"],
                     index_col=0)
    df["ex_date"] = [datetime(row["ex_year"], row["ex_month"], row["ex_day"]) for index, row in df.iterrows()]
    return df.drop(columns=["ex_year", "ex_month", "ex_day"])

#a list of instrument codes that update real time data and present with relevant ratios in dataframe format
class UpdateList(object):
    def __init__(self, code_list, index):
        assert index.lower() in ["hsi", "hscei"]
        self.code_list = code_list
        self.index = index.upper()
        self.details = get_details(f"{index.lower()}_data.csv")
        self.update_index()
        self.update_list()

    #standard sort mechanism that applies to all output
    def std_sort(self, data):
        return data.sort_values(["days_to_maturity", "ex_price", "be_rel_ask", "be_rel_bid"], ascending=[1, 0, 0,0])

    # refresh and return result in list
    def std_output(self, data):
        data['code'] = data.index
        data["be_rel_ask"] = [int(be) for be in data["be_rel_ask"]]
        data = data[['code', "bid", "ask", "ex_price", "be_rel_ask", "value%", "days_to_maturity"]]
        return data.values.tolist()

    # generate the full data of the code list
    def update_list(self):
        # derive the real time data of the code list
        data = get_real_time(self.code_list)
        df = pd.DataFrame(data).transpose()
        df.columns = column_name[:13]
        df = pd.merge(df, self.details, left_index=True, right_index=True)

        # calculate essential ratios on real time data
        ind_level = self.rt_index.iloc[0]["price"]

        # breakeven point based on ask price
        df["be_abs_ask"] = df["ex_price"] - df["ask"] * df["lot_size"]
        df["be_rel_ask"] = df["be_abs_ask"] - ind_level

        # breakeven point based on bid price
        df["be_abs_bid"] = df["ex_price"] - df["bid"] * df["lot_size"]
        df["be_rel_bid"] = df["be_abs_bid"] - ind_level

        # value based on current index status
        df["value"] = [max(0, round((row["ex_price"] - ind_level) / row["lot_size"], 3)) for index, row in
                       df.iterrows()]
        df["value%"] = [row["value"] / row["ask"] * 100 if row["ask"] != 0 else row["value"] / row["price"] * 100 for index, row in df.iterrows()]
        df["value%"] = [f"{round(perc,2)}%" for perc in df["value%"]]

        # days to maturity
        df["days_to_maturity"] = [(ex_day - datetime.today()).days for ex_day in df["ex_date"]]

        # assign the dataframe to the UpdateList object
        self.rt_codes = self.std_sort(df)

    # derive the real time index data
    def update_index(self):
        data = get_real_time(self.index)
        df = pd.DataFrame(data).transpose()
        df.columns = column_name[:13]
        self.rt_index = df

    # refresh real time data
    def refresh(self):
        self.update_index()
        self.update_list()
        return self.rt_codes

    def refresh_std(self):
        df = self.refresh()
        return self.std_output(df)

    # return the peers of a target instrument that match exactly its specifics(ex-price and maturity)
    def peer_comp_same(self, target_code):
        df = self.rt_codes
        target_details = df[df.index == target_code].iloc[0]

        df = df[df["ex_price"] == target_details["ex_price"]]
        df = df[df["days_to_maturity"] == target_details["days_to_maturity"]]

        return self.std_sort(df)


    # return the peers of a target instrument that match exactly its specifics(ex-price and maturity)
    def peer_comp_similar(self, target_code, **thresholds):
        df = self.rt_codes
        target_details = df[df.index == target_code].iloc[0]
        ex_price_thre = thresholds.get("ex_price") if thresholds.get("ex_price") else 0
        days_to_maturity_thre = thresholds.get("days_to_maturity") if thresholds.get("days_to_maturity") else 0

        df = df[abs(df["ex_price"] - target_details["ex_price"]) <= ex_price_thre]
        df = df[abs(df["days_to_maturity"] - target_details["days_to_maturity"]) <= days_to_maturity_thre]

        return self.std_sort(df)

    def imply_time_value(self, target_code, threshold=1000):
        df = self.rt_codes
        target_details = df[df.index == target_code].iloc[0]
        df_comp = df[abs(df["ex_price"] - target_details["ex_price"]) <= threshold]
        df_comp["ask_per_p"] = df_comp["ask"] / df_comp["lot_size"]
        print(df_comp[["bid", "ask", "ex_price", "lot_size", "ask_per_p", "days_to_maturity"]])

        #find the base group: group of instruments with the shortest DTM
        df_base = df_comp[df_comp["ex_date"] == df_comp.ex_date.min()]

        #find the base instrument: the cheapest among the group
        df_base["ask_per_p"] = df_base["ask"] / df_base["lot_size"]
        df_base = df_base[df_base["premium"] == df_base.premium.min()]
        base_details = df_base.iloc[0]

        compare = pd.DataFrame([target_details, base_details])
        print(compare[["bid", "ask", "ex_price", "lot_size", "be_rel_ask", "value", "value%", "days_to_maturity"]])
        value_adj = base_details["value"] - target_details["value"] #adjust the difference in value resulted from ex_price difference

        #calculate the time premium


        #return self.std_sort(df_comp)


    def index_status(self):
        status = self.rt_index.iloc[0]
        name = status["name_eng"]
        price = status["price"]
        volumn = round(status["volumn"] / 1000000, 2)
        change = "{}%".format(int((price / status["last_close"] - 1) * 10000) / 100)
        low, high = real_time_highlow(self.index)
        from_high = "-{}%".format(int((1 - price / high) * 10000) / 100)
        from_low =  "+{}%".format(int((price / low - 1) * 10000) / 100)
        return "{}: {}\t{}\t{}bn\n{} / {} from 1-month high/low".format(name, price, change, volumn, from_high, from_low)

def real_time_highlow(index):
    url = "http://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=" + index
    page = requests.get(url, headers=headers)
    tree = html.fromstring(page.content)

    month_highlow = tree.xpath("//*[@id='hkIdxContainer']/div[5]/div[8]//text()").pop().split('-')
    daily_highlow = tree.xpath("//*[@id='hkIdxContainer']/div[5]/div[2]//text()").pop().split('-')
    low = min(int(float(month_highlow[0].strip().replace(',', ''))),
              int(float(daily_highlow[0].strip().replace(',', ''))))
    high = max(int(float(month_highlow[1].strip().replace(',', ''))),
               int(float(daily_highlow[1].strip().replace(',', ''))))
    return low, high


if __name__ == "__main__":


    # to derive the list of codes to refresh
    cl = get_details("hsi_data.csv")  # get full list
    cl = cl[cl["ex_price"] <= 27000]
    cl = list(cl.index)  # convert into list

    u = UpdateList(cl, "hsi")
    #print(u.peer_comp_all(11570)[["bid", "ask", "ex_price", "be_rel_ask", "value", "value%", "days_to_maturity"]])
    #print(u.peer_comp_similar(23106, ex_price=100, days_to_maturity=5)[["bid", "ask", "ex_price", "be_rel_ask", "value", "value%", "days_to_maturity"]])
    print(u.imply_time_value(23106,100))



