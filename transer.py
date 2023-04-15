import json
import sqlite3


class TransBase:
    """转换基类"""
    def __init__(self, filename="", source_database="", init_sql="", database=":memory:"):
        if init_sql != "": self.init_sql = init_sql
        if self.init_sql == "":raise "init sql is none, but it is necessary!"
        self._conn = sqlite3.connect(database)
        self.commit_sql(self.init_sql)# 初始化表结构

        if filename:
            with open(filename, "r", encoding="utf-8") as f:  # read source json filename
                self.origin_data:list = json.load(f)
        if source_database:
            self.source_conn = sqlite3.connect(source_database)

    def commit_sql(self, sql:str, *args):
        self._conn.execute(sql, args)
        self._conn.commit()

    def select_one(self, sql:str, *args):
        return self._conn.execute(sql, *args).fetchone()[0]

    def select_many(self, sql:str, *args):
        return self._conn.execute(sql, *args).fetchall()

    def run(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(self)
        self._conn.close()
        if self.source_conn:
            self.source_conn.close()


class CaoCaoPoetry(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS caocaoshiji(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR, paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            self.commit_sql("INSERT INTO caocaoshiji (title, paragraphs) VALUES (?, ?)", poem['title'], "|".join(poem['paragraphs']))
            print(poem['title'], "is ok")
            # print(poem['paragraphs'])

cao_poetry = CaoCaoPoetry("./caocaoshiji/caocao.json")
cao_poetry.run()

class ChuCi(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS chuci(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR, `section` VARCHAR COMMENT `篇名`, author VARCHAR,  paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            self.commit_sql("INSERT INTO chuci (title, `section`, author, paragraphs ) VALUES (?, ?, ?, ?)", poem['title'],poem["section"], poem["author"],"|".join(poem['content']))
            print(poem['title'], "is ok")
            # print(poem['paragraphs'])

chuci = ChuCi("./chuci/chuci.json")
chuci.run()


class SongCi(TransBase):
    def __init__(self, *args, **kwargs):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS songci(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR COMMENT `词排名`, author VARCHAR,  paragraphs TEXT);
    """
        super().__init__(*args, **kwargs)

    def run(self):
        self.origin_data = self.source_conn.execute("SELECT rhythmic, author, content FROM ci").fetchall()
        for poem in self.origin_data:
            self.commit_sql("INSERT INTO songci (title, author, paragraphs ) VALUES (?, ?, ?)", poem[0],poem[1],"|".join(poem[2].split("\n")))
            # print(poem, "is ok")

songci = SongCi(source_database="./ci/ci.db")
songci.run()


class LunYu(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS lunyu(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR, paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            # for paragraph in poem['paragraphs']:
            #     self.commit_sql("INSERT INTO lunyu (title, paragraphs) VALUES (?, ?)", poem['chapter'], paragraph)
            self.commit_sql("INSERT INTO lunyu (title, paragraphs) VALUES (?, ?)", poem['chapter'], "|".join(poem["paragraphs"]))
            print(poem['chapter'], "is ok")

lunyu = LunYu("./lunyu/lunyu.json")
lunyu.run()


class ShiJing(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS shijing(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR, sub VARCHAR COMMENT `手法`, `section` VARCHAR COMMENT `篇名` , paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            # for paragraph in poem['paragraphs']:
            #     self.commit_sql("INSERT INTO lunyu (title, paragraphs) VALUES (?, ?)", poem['chapter'], paragraph)
            self.commit_sql("INSERT INTO shijing (title,sub ,`section`,  paragraphs) VALUES (?, ?, ?, ?)", poem['title'], poem["chapter"], poem["section"],"|".join(poem["content"]))
            print(poem['chapter'], "is ok")

shijing = ShiJing("./shijing/shijing.json")
shijing.run()


class ShuiMoTangShi(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS shuimotangshi(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR,author VARCHAR,  paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            try:
                self.commit_sql("INSERT INTO shuimotangshi (title, author, paragraphs) VALUES (?, ?, ?)", poem['title'],poem["author"], "|".join(poem['paragraphs']))
                print(poem['title'], "is ok", self.origin_data.index(poem))
            except KeyError:
                print(poem['title'], "is unuseful", self.origin_data.index(poem))
            except Exception as ret:
                print(ret, self.origin_data.index(poem))
                raise ret
            # print(poem['paragraphs'])

shuimotangshi = ShuiMoTangShi("./shuimotangshi/shuimotangshi.json")
shuimotangshi.run()


class YuanQu(TransBase):
    def __init__(self, *args):
        self.init_sql =  """
    CREATE TABLE IF NOT EXISTS yuanqu(id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR, author VARCHAR, paragraphs TEXT);
    """
        super().__init__(*args)

    def run(self):
        for poem in self.origin_data:
            # for paragraph in poem['paragraphs']:
            #     self.commit_sql("INSERT INTO lunyu (title, paragraphs) VALUES (?, ?)", poem['chapter'], paragraph)
            self.commit_sql("INSERT INTO yuanqu (title,author ,paragraphs) VALUES (?, ?, ?)", poem['title'], poem["author"], "|".join(poem["paragraphs"]))
            print(poem['title'], "is ok")

yuanqu = YuanQu("./yuanqu/yuanqu.json")
yuanqu.run()
