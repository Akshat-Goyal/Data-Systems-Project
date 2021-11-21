import os
import random
import pandas as pd
import sys


def getRandom():
    startRange = 1
    endRange = 10000
    return random.randint(startRange, endRange)


def writeTestFile():
    f = open("test1.txt", "w")
    f.write("LOAD X \n")
    f.write("LOAD Y \n")
    f.write("RS1 <- JOIN USING NESTED X, Y ON col12 == col22 BUFFER 10 \n")
    f.write("EXPORT RS1 \n")
    f.close()

    f = open("test2.txt", "w")
    f.write("LOAD X \n")
    f.write("LOAD Y \n")
    f.write("RS2 <- JOIN USING PARTHASH X, Y ON col12 == col22 BUFFER 10 \n")
    f.write("EXPORT RS2 \n")
    f.close()


def runCommands():
    os.system("../src/./server < ../data/test1.txt")
    os.system("../src/./server < ../data/test2.txt")


def compareResultFiles():
    print("\n \n COMPARING FILES \n \n")
    df1 = pd.read_csv("../data/RS1.csv", skipinitialspace=True)
    df2 = pd.read_csv("../data/RS2.csv", skipinitialspace=True)
    result = pd.read_csv("result.csv", skipinitialspace=True)

    df1.sort_values(by=df1.columns.to_list(), inplace=True)
    df2.sort_values(by=df2.columns.to_list(), inplace=True)
    result.sort_values(by=result.columns.to_list(), inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    result.reset_index(drop=True, inplace=True)

    print(df1.equals(result))
    print(df2.equals(result))
    print(df1.equals(df2))


if __name__ == "__main__":
    n1 = int(sys.argv[1])
    n2 = int(sys.argv[2])

    df1 = pd.DataFrame(columns=["col11", "col12", "col13"])
    df2 = pd.DataFrame(columns=["col21", "col22", "col23"])
    result = pd.DataFrame(
        columns=["col11", "col12", "col13", "col21", "col22", "col23"]
    )

    for i in range(n1):
        df1 = df1.append(
            {"col11": getRandom(), "col12": 9 * i + 4, "col13": getRandom()},
            ignore_index=True,
        )

    for i in range(n2):
        df2 = df2.append(
            {"col21": getRandom(), "col22": 9 * i + 4, "col23": getRandom()},
            ignore_index=True,
        )

    df1.to_csv("../data/X.csv", index=False)
    df2.to_csv("../data/Y.csv", index=False)

    for _, row1 in df1.iterrows():
        for _, row2 in df2.iterrows():
            if row1["col12"] == row2["col22"]:
                result = result.append(
                    {
                        "col11": row1["col11"],
                        "col12": row1["col12"],
                        "col13": row1["col13"],
                        "col21": row2["col21"],
                        "col22": row2["col22"],
                        "col23": row2["col23"],
                    },
                    ignore_index=True,
                )

    result.to_csv("result.csv", index=False)

    writeTestFile()
    runCommands()

    compareResultFiles()
