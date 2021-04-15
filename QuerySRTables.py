from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
import os

try:
    with connect(
        host="198.102.4.55",
        port = "3307",
        user="srdemo003",
        password=getpass("Enter password: "),
    ) as connection:
        fills_col_query = "SHOW COLUMNS FROM srtrade009.msgsrparentexecution"
        fills_row_query = "SELECT * FROM srtrade009.msgsrparentexecution WHERE accnt = 'T.SRDEMO003'"
        qwap_col_query = "SHOW COLUMNS FROM srtrade.msgsrparentbrkrstate"
        qwap_row_query = "SELECT * FROM srtrade.msgsrparentbrkrstate WHERE accnt = 'T.SRDEMO003'"
        ticket_col_query = 'SHOW COLUMNS FROM srtrade.msgsrparentbrkrdetail'
        ticket_row_query = "SELECT * FROM srtrade.msgsrparentbrkrdetail WHERE accnt = 'T.SRDEMO003'"
        with connection.cursor() as cursor:
            cursor.execute(fills_col_query)
            cols = [c[0] for c in cursor.fetchall()]
            cursor.execute(fills_row_query)
            fills = pd.DataFrame(cursor.fetchall(), columns=cols)
            cursor.execute(qwap_col_query)
            cols = [c[0] for c in cursor.fetchall()]
            cursor.execute(qwap_row_query)
            qwap = pd.DataFrame(cursor.fetchall(), columns=cols)
            cursor.execute(ticket_col_query)
            cols = [c[0] for c in cursor.fetchall()]
            cursor.execute(ticket_row_query)
            ticket = pd.DataFrame(cursor.fetchall(), columns=cols)
        saveDir = os.path.join(os.getcwd(), 'FillData')
        t = pd.Timestamp.now()
        fills.to_csv(os.path.join(saveDir, f'Trades{t:%Y%m%d}.csv'))
        qwap.to_csv(os.path.join(saveDir, f'BrkrState{t:%Y%m%d}.csv'))
        ticket.to_csv(os.path.join(saveDir, f'BrkrDetail{t:%Y%m%d}.csv'))
except Error as e:
    print(e)
    