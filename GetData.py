from ntfdl import Dl
import sqlite3
from datetime import datetime
from datetime import timedelta
import pandas as pd

class netfonds_to_database(object):
    def __init__(self):
        ### Creates database if it doesn't exist
        ### Connects to it
        self.conn = sqlite3.connect('NetFonds.db')
        self.cursor = self.conn.cursor()

    def create_table(self):
        ### CREATES TABLE TRADE
        command = """CREATE TABLE IF NOT EXISTS Trade(id_trade INTEGER NOT NULL PRIMARY KEY, datetime TEXT NOT NULL, price REAL NOT NULL,
                 volume TEXT NOT NULL, buyer STRING, seller STRING, initiator STRING);"""

        ### CREATES TABLE BOOK
        command_2 = """CREATE TABLE IF NOT EXISTS Book(id_book INTEGER NOT NULL PRIMARY KEY, datetime TEXT NOT NULL, bid INTEGER, bid_depth REAL, bid_depth_total REAL, ask INTEGER,
        ask_depth REAL, ask_depth_total REAL)"""

        ### CREATES A LIST CONTAINING ALL COMMANDS
        list_commands = [command, command_2]

        ### EXECUTES COMMANDS
        for command in list_commands:
            self.cursor.execute(command)
            self.conn.commit()

    def get_data_from_netfonds(self):

        self.df_trades = None
        self.df_book = None

        for i in range(12):
            date = str(netfonds_to_database.stl_data(delta_days=-i))
            stl = Dl('SPY.A', exchange='NSE', day=date, download=True)

            trades = stl.trades
            book = stl.positions

            ### Resets index
            trades.reset_index(drop=True, inplace=True)
            book.reset_index(drop=True, inplace=True)

            ### CREATE DF IF NOT ALREADY EXISTS:
            if self.df_trades is None:
                self.df_trades = pd.DataFrame(columns=trades.columns)
                self.df_book = pd.DataFrame(columns=book.columns)

            self.df_trades = self.df_trades.append(trades)
            self.df_book = self.df_book.append(book)

        self.df_trades.reset_index(drop=True, inplace=True)
        self.df_book.reset_index(drop=True, inplace=True)
        return self.df_trades, self.df_book

    def input_trades_into_db(self):
        print self.df_trades
        self.df_trades['time'] = self.df_trades['time'].astype(str)
        for i, row in self.df_trades.iterrows():
            # date_trade = self.df_trades.loc[i, 'time'].split(' ')[0]
            # time_trade = self.df_trades.loc[i, 'time'].split(' ')[1].replace(':', '-')
            date_trade = self.df_trades.loc[i, 'time'] + '.000'
            price = self.df_trades.loc[i, 'price']
            volume = self.df_trades.loc[i, 'volume']
            buyer = self.df_trades.loc[i, 'buyer']
            seller = self.df_trades.loc[i, 'seller']
            initiator = self.df_trades.loc[i, 'initiator']

            print date_trade
            print type(date_trade)
            # print time_trade
            # print type(time_trade)

            if str(initiator) != 'nan':
                command = """INSERT OR IGNORE INTO Trade(datetime, time, price, volume, buyer, seller, initiator) VALUES('"""\
                          + str(date_trade) + "', " + str(price) + ", " + str(volume) + ", " + str(buyer) + ", " + str(seller) + ", " + str(initiator) + """);"""
            else:
                command = """INSERT OR IGNORE INTO Trade(datetime, price, volume) VALUES('""" + str(date_trade) + "', " \
                          + str(price) + ", " + str(volume) + """);"""

            print command
            self.cursor.execute(command)
        self.conn.commit()

    def input_book_into_db(self):
        self.df_book['time'] = self.df_book['time'].astype(str)
        for i, row in self.df_book.iterrows():
            date_time = self.df_book.loc[i, 'time']
            bid = self.df_book.loc[i, 'bid']
            bid_depth = self.df_book.loc[i, 'bid_depth']
            bid_depth_total = self.df_book.loc[i, 'bid_depth_total']
            ask = self.df_book.loc[i, 'ask']
            ask_depth = self.df_book.loc[i, 'ask_depth']
            ask_depth_total = self.df_book.loc[i, 'ask_depth_total']

            command = """INSERT OR IGNORE INTO Book( datetime, bid, bid_depth, bid_depth_total, ask, ask_depth, ask_depth_total) VALUES ('""" \
                    + str(date_time) + "', " + str(bid) + ", " + str(bid_depth) + ", " + str(bid_depth_total) + \
                    ", " + str(ask) + ", " + str(ask_depth) + ", " + str(ask_depth_total) + """);"""

            print command
            self.cursor.execute(command)

        self.conn.commit()

    def update_db(self):
        """
        :return: main function
        """
        netfonds_to_database.get_data_from_netfonds(self)
        netfonds_to_database.create_table(self)
        netfonds_to_database.input_trades_into_db(self)
        netfonds_to_database.input_book_into_db(self)

    @staticmethod ### staticmethod to create proper date formats for DL
    def stl_data(delta_days=0, data='', zero_antecendo=True):
        '''
        Para avancar o numero de dias, colocar delta_days > 0.
        Para retroceder o numero de dias, colocar delta_days < 0.
        :param delta_days: dias a serem descontados
        :return: data no formato dd/mm/aaaa
        '''
        if data == '':
            now = datetime.datetime.now()
            day = (now + timedelta(days=delta_days)).day
            month = (now + timedelta(days=delta_days)).month
            year = (now + timedelta(days=delta_days)).year

        else:
            day = data.day
            month = data.month
            year = data.year

        day = str(day)
        month = str(month)
        year = str(year)

        if zero_antecendo == True:
            if int(day) < 10:
                day = '0' + day

            if int(month) < 10:
                month = '0' + month

        date = year + month + day
        return date

if __name__=='__main__':
    nd = netfonds_to_database()
    print nd.update_db()

