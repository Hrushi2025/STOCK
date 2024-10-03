import queriesConfig as qc
from config import db_config as conn

cursor = conn.cursor()

def createTableQueries():

    # Executes create table for moving average fact
    cursor.execute(qc.createTableMovingAverageFact)

    # Executes create table for rsi index fact
    cursor.execute(qc.createTableRsiIndexFact)


def truncateTableQueries():
    pass

    # Truncates data from moving average fact table
    cursor.execute(qc.truncateTableMovingAverageFact)

    # Truncates data from rsi index fact table
    cursor.execute(qc.truncateTableRsiIndexFact)



def insertTableQueries():

    # Executes moving avg query
    cursor.execute(qc.calculateMovingAverage)
    conn.commit()

    # Executes rsi index query
    cursor.execute(qc.calculateRsiIndex)
    conn.commit()


def main():

    # createTableQueries()
    #
    truncateTableQueries()

    insertTableQueries()


if __name__ == "__main__":
    main()