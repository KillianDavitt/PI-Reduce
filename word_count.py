from re import split


def process_line(words, word_dict):
    for word in words:
        if word in word_dict:
            word_dict[word] += 1
        else:
            word_dict[word] = 1


def process_dict(word_dict):
    temp_list = []
    for key, value in word_dict.items():
        temp_list.append((value, key))

    temp_list.sort()
    return temp_list


def format_print(input_list, reverse, word_num):
    if reverse:
        input_list.sort(reverse=True)

    print "\n", ("[Unique Words: " + str(word_num) + "]").center(35, "=")
    print "-"*35 + "\n", "%-16s %s %16s" % ("Word", "|", "Count"), "\n", "-"*35
    for count, word in input_list:
        print "%-16s %s %16d" % (word, "|", count)


def word_count(_file, max_to_min=False):
    txt = open(_file, "rU")
    word_dict = {}
    for line in txt:
        if line.replace(" ", "") != ("\n" or None):
            process_line(filter(None, split("[^a-zA-Z']+", line.lower())), word_dict)

    txt.close()
    final_list = process_dict(word_dict)
    format_print(final_list, max_to_min, len(word_dict))


word_count("Fifty_Shades_of_Grey_-_E_L_James.txt", False)