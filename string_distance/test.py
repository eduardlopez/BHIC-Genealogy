from string_distance import *


strings = [
            ['Hagebols',
             'Hagebolt'],

            ['Hagebols',
             'hagebold'],

            ['Heezen',
             'ezen'],

            ['PutmanCramer Adolfina Hendrietta',
             'Putman   Cramer Adolfina Hendrietta'] # 3 spaces
]

maxAllowedErrors = 2

for string in strings:
    print( get_2_strings_score(string[0], string[1]), get_2_strings_validation(string[0], string[1], 2) )

# RESULT:
# 1 True
# 2 True
# 1 True
# 3 False