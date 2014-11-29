# -*- coding: utf-8 -*-

import MySQLdb
import math
import redis
#from datetime import *
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# 打开数据库连接
db = MySQLdb.connect(host="localhost",user="ethan",passwd="ethan",db="stockoracle",charset="utf8",use_unicode=True )


# 使用cursor()方法获取操作游
cursor = db.cursor()

oneday=datetime.timedelta(days=1)
today = datetime.datetime.today()

daysbefore = 0
beginday = today - (daysbefore + 1)* oneday
endday = today - daysbefore * oneday
# 使用execute方法执行SQL语句

cursor.execute("select time, id, title, post from posts where time > %s and time < %s", (beginday, endday))

# 使用 fetchone() 方法获取一条数据库。
data = cursor.fetchall()
#我们要统计的最大的词的长度
N = 5

totallen = 0 #参与统计的文本总长度
wordlist = []
postlist = {}
for post in data:
    time = post[0]
    id = post[1]
    title = post[2]
    text = post[3]
    textlen = len(text)
    postlist[id] = text
    totallen = totallen + textlen
    print time, id, title, text

    for i in xrange(textlen):
        for j in xrange(N + 1): #因为要计算左右的信息熵，所以需要多加一个
            k = i + j + 1
            if k <= textlen:
                wordlist.append(text[i : k])
print "totallen =" , totallen 
wordlist.sort()
wordslen = len(wordlist)

kvdbfreq = {}
kvdbleft = {}
kvdbright = {}
thisword = ''
thiscount = 0
thislen = 0
lastword = wordlist[0]
lastlen = len(lastword)
lastcount = 1

for i in xrange(1, wordslen):
    thisword = wordlist[i]
    thislen = len(thisword)
    #pdb.set_trace()
    if thisword == lastword:
        thiscount = lastcount + 1
    else:
        #print lastword, lastcount
        thiscount = 1
        if thislen > 2 and thislen > lastlen: #只对双字词以上统计自由度
            kvdb = {} #存放右字的频数
            rightsum = 0
            for j in xrange(i, wordslen):
                word = wordlist[j]
                #print lastword, word
                if word[0 : lastlen] == lastword:
                    rightsum = rightsum + 1
                    right = word[lastlen : lastlen + 1]
                    #print word[0 : lastlen], lastword, right
                    kvdb[right] = kvdb.get(right, 0) + 1
                else: #不再是以lastword为开头的词了
                    shang = 0.0 #熵
                    for key in kvdb:
                        val = kvdb[key]
                        freq = (val + 0.0) / rightsum
                        shang = shang + ( - freq * math.log(freq))
                    kvdbright[lastword] = shang #计算出这个词的右熵
                    #print lastword, shang
                    break  
        #end if
        if lastlen <= N: #因为wordlist里存着超过比我们需要的词还要长一位的词，这个词不统计
            freq = lastcount
            kvdbfreq[lastword] = freq #计算出这个词的频率
    lastcount = thiscount
    lastword = thisword
    lastlen = thislen

#统计左边自由度
for i in xrange(wordslen):
    wordlist[i] = wordlist[i][::-1]
wordlist.sort()
wordslen = len(wordlist)

thisword = ''
thiscount = 0
thislen = 0
lastword = wordlist[0]
lastlen = len(lastword)
lastcount = 1

for i in xrange(1, wordslen):
    thisword = wordlist[i]
    thislen = len(thisword)
    #pdb.set_trace()
    if thisword == lastword:
        thiscount = lastcount + 1
    else:
        #print lastword, lastcount
        thiscount = 1
        if thislen > 2 and thislen > lastlen: #只对双字词以上统计自由度
            kvdb = {} #存放右字的频数
            rightsum = 0
            for j in xrange(i, wordslen):
                word = wordlist[j]
                #print lastword, word
                if word[0 : lastlen] == lastword:
                    rightsum = rightsum + 1
                    right = word[lastlen : lastlen + 1]
                    #print word[0 : lastlen], lastword, right
                    kvdb[right] = kvdb.get(right, 0) + 1
                else: #不再是以lastword为开头的词了
                    shang = 0.0 #熵
                    for key in kvdb:
                        val = kvdb[key]
                        freq = (val + 0.0) / rightsum
                        shang = shang + ( - freq * math.log(freq))
                    kvdbleft[lastword[::-1]] = shang #计算出这个词的左熵
                    #print lastword, shang
                    break  
        #end if
    lastcount = thiscount
    lastword = thisword
    lastlen = thislen

kvdblog = {}
for key in kvdbfreq:
    keylen = len(key)
    val = kvdbfreq[key]
    if(keylen < 2):
        continue
    if val < 10:  #对关键词的出现次数做一个筛选
        continue
    maxp = 0.0
    for i in xrange(keylen - 1):
        keyleft = key[0:i+1]
        keyright = key[i+1:keylen]
        p0 = (kvdbfreq[keyleft] + 0.0) / totallen
        p1 = (kvdbfreq[keyright] + 0.0) / totallen
        #print key, keyleft, keyright, p0, p1
        p = p0 * p1
        if p > maxp:
            maxp = p
        val = (kvdbfreq[key] + 0.0) / totallen
    freq = math.log(val/maxp, 2)
    kvdblog[key] = freq
    #print key, freq 

rankfreq = sorted(kvdblog.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
for i in xrange(len(rankfreq)):
    key = rankfreq[i][0]
    try:
#        if rankfreq[i][1] > 5.0 and kvdbleft[key] > 2.0 and kvdbright[key] > 2.0: #根据结合度和自由度做筛选
            #print str(endday)[:10], key, kvdbfreq[key], kvdblog[key], kvdbleft[key], kvdbright[key]
            sql = 'INSERT INTO WORDS(RQ, WORD, HITS, FREQUENCY, FREELEFT, FREERIGHT) VALUES(%s, %s, %s, %s, %s, %s)'
            params = (str(endday)[:10], key, kvdbfreq[key], kvdblog[key], kvdbleft[key], kvdbright[key])
            cursor.execute(sql, params)
    except Exception, e:
        print e
db.commit()

if daysbefore == 0:
    cursor.execute("select id, word, hits from words where rq = %s and frequency > 5.0 and freeleft > 2.0 and freeright > 2.0", (endday.date(),))
    data = cursor.fetchall()
    words = {}
    for word in data:
        wid = word[0]
        key = word[1]
        hits = word[2]
        scores = [wid, hits, 0, 0.0, 0]
        words[key] = scores
    cursor.execute("select id, word, hits from words where rq = %s", (beginday.date(),))
    data = cursor.fetchall()
    for word in data:
        key = word[1]
        hits = word[2]
        scores = words.get(key)
        if scores == None:
            continue
        scores[2] = hits
        words[key] = scores

    totalhits = 0
    totalscore = 0.0
    for word in words:
        scores = words[word]
        hits0 = scores[1]
        hits1 = scores[2] #前一天的关键词数目
        scores[3] = (hits0 + 0.0) / (hits0 + hits1)
        totalhits = totalhits + (hits0 + hits1)
        totalscore = totalscore + scores[3]
    avragehits = (totalhits + 0.0) / len(words)
    avragescore = totalscore / len(words)
    print avragehits, avragescore

    for word in words:
        scores = words[word]
        wid = scores[0]
        hits0 = scores[1]
        hits1 = scores[2] 
        score = scores[3]
        scores[3] = (score * (hits0 + hits1) + avragescore * avragehits) / (hits0 + hits1 + avragehits)
        #print wid, word, scores[3]
        cursor.execute("update words set mark = %s, hits1 = %s where id = %s" % (scores[3], hits1, wid))
    db.commit()

    print "begin calc"
    cursor.execute("truncate table ptwds") #每次都清空该表，其实和临时表差不多
    for pid in postlist:
        text = postlist[pid]
        for word in words:
            scores = words[word]
            wid = scores[0]          
            wdhits = text.count(word) #得到该关键词在文章中提到的次数
            pthits = 0
            if wdhits > 0:
                pthits = 1
            scores[4] = scores[4] + pthits #该关键词在文章中有提到 
            words[word] = scores
            cursor.execute("replace into ptwds values(%s, %s, %s, %s)", (pid, wid, wdhits, 0.0))
            #print pid, wid, wdhits
    
    for word in words:
        scores = words[word]
        cursor.execute("update words set pthits = %s where id = %s", (scores[4],scores[0]))
        #print word, scores[4]
    db.commit()
  
    ##to do: 第二遍，计算权重
    cursor.execute("select ptid, wdid, pthits, wdhits from ptwds, words where ptwds.wdid = words.id and  wdhits > 0")
    print "second calc..."
    ptwds = cursor.fetchall()
    postlen = len(postlist)
    for pw in ptwds:
        ptid = pw[0]
        wdid = pw[1]
        pthits = pw[2]
        wdhits = pw[3]
        weight = (wdhits + 0.0) * math.log(postlen / pthits, 10)
        cursor.execute("update ptwds set weight = %s where ptid = %s and wdid = %s", (weight, ptid, wdid))
    db.commit()

    F = 20 #每篇文章取的权重最大的前F个作为文章特征值
    cursor.execute("select ptid, wdid, weight from ptwds where weight > 0 order by ptid asc, weight desc")
    wts = cursor.fetchall()
    wordshots = {}
    lastid = 0
    fcount = 0
    for wt in wts:
        ptid = wt[0]
        wdid = wt[1]
        if ptid == lastid:
            fcount = fcount + 1
        else:
            fcount = 1
        if fcount <= F:
            wordshots[wdid] = wordshots.get(wdid, 0) + 1
        lastid = ptid

    for wdid in wordshots:
        cursor.execute("update words set hits2 = %s where id = %s", (wordshots[wdid], wdid))
    db.commit()

    if daysbefore == 0:
        cursor.execute("select id, word, hits2 from words where rq = %s and hits2 > 0", (endday.date(),))
        data = cursor.fetchall()
        words = {}
        for word in data:
            wid = word[0]
            key = word[1]
            hits = word[2]
            scores = [wid, hits, 0, 0.0, 0]
            words[key] = scores
        cursor.execute("select id, word, hits2 from words where rq = %s", (beginday.date(),))
        data = cursor.fetchall()
        for word in data:
            key = word[1]
            hits = word[2]
            if hits == None:
                hits = 0
            scores = words.get(key)
            if scores == None:
                continue
            scores[2] = hits
            words[key] = scores

        totalhits = 0
        totalscore = 0.0
        for word in words:
            scores = words[word]
            hits0 = scores[1]
            hits1 = scores[2] #前一天的关键词数目
            #print word, hits0, hits1
            scores[3] = (hits0 + 0.0) / (hits0 + hits1)
            totalhits = totalhits + (hits0 + hits1)
            totalscore = totalscore + scores[3]
        avragehits = (totalhits + 0.0) / len(words)
        avragescore = totalscore / len(words)
        print avragehits, avragescore

        for word in words:
            scores = words[word]
            wid = scores[0]
            hits0 = scores[1]
            hits1 = scores[2] 
            score = scores[3]
            scores[3] = (score * (hits0 + hits1) + avragescore * avragehits) / (hits0 + hits1 + avragehits)
            #print wid, word, scores[3]
            cursor.execute("update words set score = %s, hits3 = %s where id = %s" % (scores[3], hits1, wid))
        db.commit()


# 关闭数据库连接
db.close()