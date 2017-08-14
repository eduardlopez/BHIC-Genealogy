__author__ ="eduardlopez"

# Installation instructions:
#
# 1) easy_install python-Levenshtein
#
# 2) In windows you need to have installed: "Microsoft Visual C++ 14.0"
# You can downloaded from here: https://wiki.python.org/moin/WindowsCompilers#Microsoft_Visual_C.2B-.2B-_14.0_standalone:_Visual_C.2B-.2B-_Build_Tools_2015_.28x86.2C_x64.2C_ARM.29
#


from Levenshtein import distance

def get_2_strings_validation(string1, string2, maxAllowedErrors ):
    return True if distance(string1, string2) <= maxAllowedErrors else False

def get_2_strings_score(string1, string2 ):
    return distance(string1, string2)

