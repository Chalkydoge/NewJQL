myjql : myjql.c helper.c
	gcc -o myjql myjql.c
	gcc -o help helper.c
clean :
	rm -rf myjql help