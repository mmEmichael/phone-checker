# функция заглушка
def geo(numb_list):
    output_list = []
    for i in range(len(numb_list)):
        numb = numb_list[i] 
        if numb[0] == '7':
            output_list.append("RUS")
        else:
            output_list.append("NONERUS")
    return output_list