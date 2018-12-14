from lxml import html
import requests
import pandas as pd
from datetime import datetime

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
column_name = ['name_eng', 'name_chi', 'today_open', 'last_close', 'today_high', 'today_low', 'price', '?1', '?2',
               'bid', 'ask', 'volumn', 'amount', '买二量/股', '买二价', '买三量/股', '买三价', '买四量/股', '买四价', '买五量/股', '买五价',
               '卖一量/股', '卖一价', '卖二量/股', '卖二价', '卖三量/股', '卖三价', '卖四量/股', '卖四价', '卖五量/股', '卖五价', '日期', '时间']

# convert list of codes into sina stock api url
def gen_url(input: list) -> str:
    return "http://hq.sinajs.cn/list=" + \
           ",".join(["rt_hk{}".format(str(code).zfill(5)) if isinstance(code, int) else "rt_hk{}".format(str(code)) for code in input])

# extract data from the url
def get_real_time(codes: list) -> dict:
    # to get data from api
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
    #sample output:
    # {20360: ['HSHSCEI@EP1812A', '中企汇丰八乙沽Ａ', 0.082, 0.044, 0.082, 0.082, 0.082, 0.038, 86.364, 0.083, 0.089, 24600.0, 300000.0],
    # 23106: ['HS-HSI @EP1901D', '恒指汇丰九一沽Ｄ', 0.11, 0.082, 0.13, 0.11, 0.13, 0.048, 58.537, 0.133, 0.14, 391750.0, 3140000.0]}

#import the basic data of the instruments
def get_details(file: str) -> pd.DataFrame:
    df = pd.read_csv(file, sep=",", index_col=1, header=0, names=["code", "ex_price", "ex_date", "lot_size"])
    df["ex_date"] = pd.to_datetime(df["ex_date"], format="%Y-%M-%d")
    df["ex_price"] = [int(pr.replace(",","").replace(".000","")) for pr in df["ex_price"]]
    df["lot_size"] = [int(si) for si in df["lot_size"]]
    return df.drop(columns=["code"])

#a list of instrument codes that update real time data and present with relevant ratios in dataframe format
class UpdateList(object):
    def __init__(self, code_list, index, **kwargs):
        assert index.lower() in ["hsi", "hscei"]

        std_output_columns = kwargs.get("std_output_columns",['code', "bid", "ask", "ex_price", "be_rel_ask", "value%", "days_to_maturity"])
        data_file = kwargs.get("data_file", f"{index.lower()}_put_data.csv")

        self.std_output_columns = std_output_columns
        self.code_list = code_list
        self.index = index.upper()
        self.details = get_details(data_file)
        print("Instrument details loaded:\n")
        print(self.details)

        self.update_index()
        self.update_list()

    #standard sort mechanism:
    # sort by days to maturity first, than ex_price, than breakeven (b/e) relative to ask price, than b/e relative to bid price
    def std_sort(self, data):
        return data.sort_values(["days_to_maturity", "ex_price", "be_rel_ask", "be_rel_bid"], ascending=[1, 0, 0,0])

    #standard output mechanism:
    def std_output(self, data):
        data['code'] = data.index
        data["be_rel_ask"] = [int(be) for be in data["be_rel_ask"]]
        data = data[self.std_output_columns]
        return data.values.tolist()
        #sample output:
        # [[20463, 0.2, 0.201, 11200, -66, '91.54%', 21],
        # [20360, 0.079, 0.085, 10688, -156, '58.82%', 21]]

    # generate the full data of the code list
    def update_list(self):
        # derive the real time data of the code list
        data = get_real_time(self.code_list)
        df = pd.DataFrame(data).transpose()
        df.columns = column_name[:13]
        print(df)
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
        data = get_real_time([self.index])
        df = pd.DataFrame(data).transpose()
        df.columns = column_name[:13]
        self.rt_index = df

    # refresh real time data and return every instrument
    def refresh(self):
        self.update_index()
        self.update_list()
        return self.std_output(self.rt_codes)

    # return the peers of a target instrument that match exactly its specifics(ex-price and maturity)
    def peer_comp_same(self, target_code):
        df = self.rt_codes
        target_details = df[df.index == target_code].iloc[0]

        df = df[df["ex_price"] == target_details["ex_price"]]
        df = df[df["days_to_maturity"] == target_details["days_to_maturity"]]

        return self.std_output(self.std_sort(df))

    # return the peers of a target instrument that match exactly its specifics(ex-price and maturity)
    def peer_comp_similar(self, target_code, **thresholds):
        df = self.rt_codes
        target_details = df[df.index == target_code].iloc[0]
        ex_price_thre = thresholds.get("ex_price") if thresholds.get("ex_price") else 0
        days_to_maturity_thre = thresholds.get("days_to_maturity") if thresholds.get("days_to_maturity") else 0

        df = df[abs(df["ex_price"] - target_details["ex_price"]) <= ex_price_thre]
        df = df[abs(df["days_to_maturity"] - target_details["days_to_maturity"]) <= days_to_maturity_thre]

        return self.std_output(self.std_sort(df))

if __name__ == "__main__":

    d = get_details("hsi_put_data.csv")
    cl = list(d.index)
    u = UpdateList(cl,"HSI")
    print(u.peer_comp_same(11414))
