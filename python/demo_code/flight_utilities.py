
def get_year_range(desc_str: object):
    return from_to


# def get_year_range(desc_str: object):
#     start_idx = str.rfind(desc_str, '(')
#     end_idx = str.rfind(desc_str, ')')

#     # couldn't find parentheses
#     if start_idx == -1 or end_idx == -1:
#         raise ValueError("No year range was found in ".join(desc_str))

#     from_to = []

#     for s in str.split(desc_str[start_idx+1:end_idx], '-'):
#         from_to.append(None if s.strip() == '' else int(s))
    
#     return from_to