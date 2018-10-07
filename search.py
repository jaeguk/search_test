import re

import json
import math, struct, sys
import os.path

#from konlpy.tag import Kkma
#_analyzer= Kkma()

from konlpy.tag import Mecab
_analyzer= Mecab()

def xplit(value):
    return re.split('\r\n|\n', value)


def parse_nouns(did, text, dic_terms, f):
    candidates = xplit(text.strip())

    for candidate in candidates:
        if len(candidate):
            nouns = _analyzer.nouns(candidate)
            for noun in nouns:
                value = dic_terms.get(noun, 0)
                dic_terms[noun] = value + 1

    #terms_list = list(dic_terms.keys()).sort()



def forward_indexing():
    with open("fnames.txt", 'r') as f:
        lines = f.readlines()
        with open("out/TF.txt", 'w') as tf_file:
            for did, line in enumerate(lines):

                line = line.replace('\n', '')
                try:
                    with open("./ITnews/" + line, 'r', encoding='utf-8') as f_item:
                        dic_terms = dict()

                        #print(line)
                        while f_item:
                            line = f_item.readline()
                            if not line: break
                            parse_nouns(did, line, dic_terms, tf_file)

                        #print(dic_terms)

                        out = {did+1 : dic_terms}
                        json_string = json.dumps(out)
                        tf_file.write(json_string + '\n')

                except FileNotFoundError:
                    print("file is not exist" + line)



def make_invertedFile():
    dics_tf = dict()
    invert_list = []

    with open("out/TF.txt", 'r') as f:
        i = 1;

        while f:
            line = f.readline()
            if not line: break
            dics = json.loads(line)

            #print(dics)
            dic_terms = dics[str(i)]

            did = i
            for term in dic_terms.keys():
                current = dic_terms[term]

                if term in dics_tf:
                    old = dics_tf[term]
                    new = dict()

                    new['df'] = old['df'] + 1
                    new['tf_list'] = [x for x in old['tf_list']]

                    new['tf_list'].append(
                        {
                            'did' : did,
                            'tf' : current,
                            'weight' : 0
                        }
                    )
                    dics_tf[term] = new
                else:
                    tf_list = []

                    tf_list.append({'did':did, 'tf':current, 'weight':0})
                    value = {"tf_list": tf_list, "df": 1}
                    dics_tf[term] = value

            i += 1

            #if i > 10 :
            #   break

        N = i - 1 # total doc count
        print("total N = ", N)

        invert_list = list(dics_tf.keys())

        start_pos = 0
        f_tf = open('out/TEMP.txt', 'w')
        f_term_table = open('out/term_table.txt', 'w')
        f_posting_file = open('out/posting_file.txt', 'wb')

        term_tables = dict()
        for j,x in enumerate(sorted(invert_list)):
            doc_count = len(dics_tf[x]["tf_list"])

            dic_item = dics_tf[x]
            tf_list = [x for x in dic_item["tf_list"]]

            for tf_item in tf_list:

                #weight = tf_item['tf'] / math.log( (N+1) / dic_item['df'], 2)
                weight = tf_item['tf'] / math.log( (N) / dic_item['df'], 2)

                tf_item['weight'] = weight

            #print(x, tf_list)

            sort_on = "weight"
            decorated = [(dict_[sort_on], i) for i, dict_ in enumerate(tf_list)]

            decorated.sort(reverse=True)

            sorted_tf_list = [tf_list[index] for key, index in decorated]
            dic_item['tf_list'] = sorted_tf_list

            item = {"start_pos": start_pos, "doc_count": doc_count}
            print(x, item, dic_item)

            # write TF file
            tf_temp = dict()
            tf_temp['term'] = x
            tf_temp['termID'] = j + 1
            tf_temp['tf_item'] = dic_item

            f_tf.write(json.dumps(tf_temp) + '\n')

            term_tables[x] = item

            # write posting file
            for tf_ in sorted_tf_list:
                did = tf_['did']
                weight = tf_['weight']

                # packing 4 byte format integer : big endian >
                f_posting_file.write(struct.pack('>i', did))

                # packing 8 byte format float ( ctype double ) : big endian >
                f_posting_file.write(struct.pack('>d', weight))

            # integer 4 byte + float 8 byte : did, weight
            start_pos += doc_count * 12

        # write term table
        f_term_table.write(json.dumps(term_tables) + '\n')

        f_term_table.close()
        f_tf.close()
        f_posting_file.close()

def search_doc(word, result):
    with open('out/term_table.txt', 'r') as f:
        str = f.readline()
        if str:
            term_tables = json.loads(str)
            #print(term_tables)
            if word in term_tables:
                item = term_tables[word]
                #print(item)

                with open('out/posting_file.txt', 'rb') as f_posting:
                    f_posting.seek(item['start_pos'])
                    remain = item['doc_count'] * 12

                    while remain > 0:
                        did_bytes = f_posting.read(4)
                        did = struct.unpack('>i', did_bytes)[0]

                        weight_bytes = f_posting.read(8)
                        weight = struct.unpack('>d', weight_bytes)[0]

                        remain -= 12

                        sim = 1
                        if did in result:
                            old = result[did]
                            sim = old['sim'] + 1

                        result[did] = {'weight': weight, 'sim': sim}


if __name__ == "__main__":

    with open("fnames.txt", 'r') as f:
        doc_names = f.readlines()

        strs =""

        for i in range(len(sys.argv)):
            if i == 0 :
                continue
            else:
                strs += " "
            strs += sys.argv[i]

        nouns = _analyzer.nouns(strs)

        result = dict()
        print(strs, nouns)

        if not os.path.isfile('out/term_table.txt'):
            forward_indexing()
            make_invertedFile()

        for noun in nouns:
            search_doc(noun, result)

        #print(result)
        lis = [
            {
                'did':key, 'doc_name': doc_names[key - 1], 'weight':result[key]['weight'], 'sim':result[key]['sim']
             } for key in result.keys()]

        for l in lis:
            # and operation
            if l['sim'] < len(nouns):
                l['sim'] = 0

        results = sorted(lis, key=lambda i: (i['sim'], i['weight']), reverse=True)

        for r in results:
            print(r)

