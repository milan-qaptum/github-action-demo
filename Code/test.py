import facebook

access_token = "EAAFIt3SFZCd8BAJYeNtL7ZAgQ0lWIEmISDrhx8DYU4iY2uUa4YZBCHxEoCj0wjDoklFVUUpgZAhkuhZAHG13XqZBpncrUR0EAyyW8KaeWc08XCf66aDAtmVZCMzrGMepI5JkDG0jX0ltx3MwoiHtuAZCk2SuubbC3alLBihGCMUQLELLk0Isp2BZC"
username = "unicefmexico"

while True:
	try:
		graph = facebook.GraphAPI(access_token, version='3.1')
		# while True:
		# 	try:
		print(username, 'username')
		profile = graph.get_object(username)
	except facebook.GraphAPIError as e:
		print(e.code)
