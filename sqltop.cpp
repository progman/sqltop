//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// 0.0.1
// Alexey Potehin <gnuplanet@gmail.com>, http://www.gnuplanet.ru/doc/cv
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
#include <errno.h>
#include <fcntl.h>
#include <list>
#include <map>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "submodule/libcore.cpp/libcore.hpp"
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
namespace global
{
	enum { SORT_MIN, SORT_MAX, SORT_REAL } sort_type = global::SORT_MIN; // sort type


	struct sql_stat_t
	{
		uint64_t    count;
		uint64_t    work_time_min;
		uint64_t    work_time_max;
		uint64_t    work_time_all;
		std::string sql_tag;
		std::string sql_str;
		bool        flag_valid;

		sql_stat_t()
		{
			this->count         = 0;
			this->work_time_min = uint64_t(-1);
			this->work_time_max = 0;
			this->work_time_all = 0;
			this->flag_valid    = false;
		}

		void add(const uint64_t work_time, const std::string &sql_tag, const std::string &sql_str)
		{
			this->count++;
			if (work_time < work_time_min)
			{
				work_time_min = work_time;
			}
			if (work_time > work_time_max)
			{
				work_time_max = work_time;
			}

			if (global::sort_type == global::SORT_MIN)
			{
				this->work_time_all = work_time_min * count;
			}

			if (global::sort_type == global::SORT_MAX)
			{
				this->work_time_all = work_time_max * count;
			}

			if (global::sort_type == global::SORT_REAL)
			{
				this->work_time_all += work_time;
			}

			if (this->flag_valid == false)
			{
				this->sql_tag    = sql_tag;
				this->sql_str    = sql_str;
				this->flag_valid = true;
			}
		}

		static bool sort1(const sql_stat_t& x1, const sql_stat_t& x2)
		{
			if (x1.work_time_all > x2.work_time_all)
			{
				return true;
			}
			return false;
		}

		static bool sort2(const sql_stat_t& x1, const sql_stat_t& x2)
		{
			if (global::sort_type == global::SORT_MIN)
			{
				if (x1.work_time_min > x2.work_time_min)
				{
					return true;
				}
			}

			if (global::sort_type == global::SORT_MAX)
			{
				if (x1.work_time_max > x2.work_time_max)
				{
					return true;
				}
			}

			if (global::sort_type == global::SORT_REAL)
			{
				if (((x1.work_time_min + x1.work_time_max) / 2) > ((x2.work_time_min + x2.work_time_max) / 2))
				{
					return true;
				}
			}

			return false;
		}
	};

	std::map<std::string, global::sql_stat_t> sql_stat_map;     // map for all querys
	std::list<global::sql_stat_t> sql_stat_list;                // list for aggregate querys
	uint64_t sql_stat_list_size                            = 0;

	uint64_t max_size_sql_tag                              = 0;
	uint64_t max_size_count                                = 0;
	uint64_t max_size_work_time_all                        = 0;
	uint64_t max_size_persent                              = 0;

	uint64_t total_work_time                               = 0;

	uint64_t unixtime_min                                  = 0;
	uint64_t unixtime_max                                  = 0;
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// show table
void show_table()
{
	uint64_t j;


// draw top
	printf("+-");
	for (size_t i=0; i < global::max_size_sql_tag; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_count; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_work_time_all; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_persent; i++)
	{
		printf("-");
	}
	printf("-+\n");


// draw body
	j = 0;
	for (std::list<global::sql_stat_t>::iterator i = global::sql_stat_list.begin(); i != global::sql_stat_list.end(); ++i)
	{
		std::string sql_tag = (*i).sql_tag;
		if (sql_tag.size() < global::max_size_sql_tag)
		{
			std::string tmp(global::max_size_sql_tag - sql_tag.size(), ' ');
			tmp.append(sql_tag);
			sql_tag = tmp;
		}


		std::string count_str;
		libcore::uint2str(count_str, (*i).count);
		if (count_str.size() < global::max_size_count)
		{
			std::string tmp(global::max_size_count - count_str.size(), ' ');
			tmp.append(count_str);
			count_str = tmp;
		}


		std::string work_time_all_str;
		libcore::uint2str(work_time_all_str, (*i).work_time_all);
		if (work_time_all_str.size() < global::max_size_work_time_all)
		{
			std::string tmp(global::max_size_work_time_all - work_time_all_str.size(), ' ');
			tmp.append(work_time_all_str);
			work_time_all_str = tmp;
		}


		char buf[1024];
		sprintf(buf, "%02.02f", (*i).work_time_all / (global::total_work_time / 100.0));
		std::string persent_str = buf;
		if (persent_str.size() < global::max_size_persent)
		{
			std::string tmp(global::max_size_persent - persent_str.size(), ' ');
			tmp.append(persent_str);
			persent_str = tmp;
		}


		printf("| %s | %s | %s | %s |\n", sql_tag.c_str(), count_str.c_str(), work_time_all_str.c_str(), persent_str.c_str());


		if ((global::sql_stat_list_size > 10) && (j == 10))
		{
// draw skip
			printf("| ");
			for (size_t i=0; i < global::max_size_sql_tag; i++)
			{
				printf(".");
			}
			printf(" | ");
			for (size_t i=0; i < global::max_size_count; i++)
			{
				printf(".");
			}
			printf(" | ");
			for (size_t i=0; i < global::max_size_work_time_all; i++)
			{
				printf(".");
			}
			printf(" | ");
			for (size_t i=0; i < global::max_size_persent; i++)
			{
				printf(".");
			}
			printf(" |\n");

			break;
		}
		j++;
	}


// draw bottom
	printf("+-");
	for (size_t i=0; i < global::max_size_sql_tag; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_count; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_work_time_all; i++)
	{
		printf("-");
	}
	printf("-+-");
	for (size_t i=0; i < global::max_size_persent; i++)
	{
		printf("-");
	}
	printf("-+\n");
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// scan SQL_STAT_FILE, aggregate data, sort and show
int scan(void *pmmap_void, size_t size, const char *psqltag)
{
//1402382914 1 SQL001 53454c454354204e4f5728293b0a
//p1
//          p2
//            p3
//                   p4
//                                                p5


	char *pcur = (char *)pmmap_void;
	char *p1 = pcur;
	char *p2 = NULL;
	char *p3 = NULL;
	char *p4 = NULL;


	uint64_t line_count = 0;
	for (;;pcur++)
	{
		if (uint64_t(pcur - ((char *)pmmap_void)) == size)
		{
			break;
		}


		if (*pcur == ' ')
		{
			if (p2 == NULL)
			{
				p2 = pcur;
				continue;
			}

			if (p3 == NULL)
			{
				p3 = pcur;
				continue;
			}

			if (p4 == NULL)
			{
				p4 = pcur;
				continue;
			}
		}


		if (*pcur == '\n')
		{
			if ((p2 == NULL) || (p3 == NULL) || (p4 == NULL))
			{
				printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col list\n", line_count + 1);
				return -1;
			}


			uint64_t    unixtime;
			uint64_t    work_time;
			std::string sql_tag(p3 + 1, p4 - p3 - 1);
			std::string sql_str(p4 + 1, pcur - p4 - 1);


			if (psqltag != NULL)
			{
				if (strcmp(psqltag, sql_tag.c_str()) == 0)
				{
// convert hex to bin, write to stdout
					char ch1, ch2;
					uint8_t r1, r2, out;

					size_t sql_str_half_size = sql_str.size() / 2;
					if ((sql_str_half_size * 2) != sql_str.size())
					{
						printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col4\n", line_count + 1);
						return -1;
					}

					const char *p = sql_str.c_str();
					for (size_t i=0; i < sql_str_half_size; ++i)
					{
						ch1 = *p;
						p++;

						ch2 = *p;
						p++;

						if (libcore::hex2bin((uint8_t)ch1, r1) == false)
						{
							printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col4\n", line_count + 1);
							return -1;
						}
						if (libcore::hex2bin((uint8_t)ch2, r2) == false)
						{
							printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col4\n", line_count + 1);
							return -1;
						}

						out = (uint8_t)((r1 << 4) + r2);

						putchar(out);
					}

					return 0;
				}
			}


			if (libcore::str2uint(unixtime, p1, p2 - p1) == false)
			{
				printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col1\n", line_count + 1);
				return -1;
			}
			if (libcore::str2uint(work_time, p2 + 1, p3 - p2 - 1) == false)
			{
				printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col2\n", line_count + 1);
				return -1;
			}
			if (libcore::is_hex(sql_str, false) == false)
			{
				printf("ERROR: invalid format SQL_STAT_FILE in row %lu, broken col4\n", line_count + 1);
				return -1;
			}


			global::sql_stat_t sql_stat = global::sql_stat_map[sql_tag];
			sql_stat.add(work_time, sql_tag, sql_str);
			global::sql_stat_map[sql_tag] = sql_stat;


			if (global::unixtime_min == 0)
			{
				global::unixtime_min = unixtime;
			}
			else
			{
				global::unixtime_max = unixtime;
			}


			p1 = pcur + 1;
			p2 = NULL;
			p3 = NULL;
			p4 = NULL;
			line_count++;
		}
	}


// sql tag not found
	if (psqltag != NULL)
	{
		printf("ERROR: sql tag not found\n");
		return -1;
	}


// create list from map
	for (std::map<std::string, global::sql_stat_t>::iterator i = global::sql_stat_map.begin(); i != global::sql_stat_map.end(); ++i)
	{
		global::sql_stat_list.push_back((*i).second);
		global::sql_stat_list_size++;
	}


// get total time
	for (std::list<global::sql_stat_t>::iterator i = global::sql_stat_list.begin(); i != global::sql_stat_list.end(); ++i)
	{
		global::total_work_time += (*i).work_time_all;
	}


// find max columns size
	for (std::list<global::sql_stat_t>::iterator i = global::sql_stat_list.begin(); i != global::sql_stat_list.end(); ++i)
	{
		std::string sql_tag = (*i).sql_tag;
		if (sql_tag.size() > global::max_size_sql_tag)
		{
			global::max_size_sql_tag = sql_tag.size();
		}


		std::string count_str;
		libcore::uint2str(count_str, (*i).count);
		if (count_str.size() > global::max_size_count)
		{
			global::max_size_count = count_str.size();
		}


		std::string work_time_all_str;
		libcore::uint2str(work_time_all_str, (*i).work_time_all);
		if (work_time_all_str.size() > global::max_size_work_time_all)
		{
			global::max_size_work_time_all = work_time_all_str.size();
		}


		char buf[1024];
		sprintf(buf, "%02.02f", (*i).work_time_all / (global::total_work_time / 100.0));
		std::string persent_str = buf;
		if (persent_str.size() > global::max_size_persent)
		{
			global::max_size_persent = persent_str.size();
		}
	}


// sort sql query by total time
	global::sql_stat_list.sort(global::sql_stat_t::sort1);
	printf("sort by execution time (group querys):\n");
	show_table();


// sort sql query by average time
	global::sql_stat_list.sort(global::sql_stat_t::sort2);
	printf("sort by excution time (single query):\n");
	show_table();


	printf("col1: sql tag\n");
	printf("col2: query count\n");
	printf("col3: execution time all query\n");
	printf("col4: persent of all time\n");
	printf("\n");


	printf("total query count : %lu\n", line_count);
	printf("total query time  : %lu\n", global::total_work_time);


	uint64_t delta_unixtime = global::unixtime_max - global::unixtime_min;
	if (delta_unixtime == 0)
	{
		printf("total log time    : %lu\n", 0L);
		printf("load time         : 100.00%%\n");
	}
	else
	{
		printf("total log time    : %lu\n", delta_unixtime * 1000000);
		printf("load time         : %02.02f%%\n", (global::total_work_time) / ((delta_unixtime * 1000000) / 100.0));
	}


	return 0;
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// open sql stat
int do_it(const char *pfilename, const char *psqltag)
{
	int rc;
	int scan_rc;


// open file
	rc = open(pfilename, O_LARGEFILE | O_RDONLY);
	if (rc == -1)
	{
		printf("ERROR[open()]: %s\n", strerror(errno));
		return -1;
	}
	int fd = rc;


// get file size
	struct stat stat_buf;
	rc = fstat(fd, &stat_buf);
	if (rc == -1)
	{
		printf("ERROR[fstat()]: %s\n", strerror(errno));
		close(fd);
		return -1;
	}
	uint64_t size = stat_buf.st_size;


// mmap file
	void *pmmap_void = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (pmmap_void == MAP_FAILED)
	{
		printf("ERROR[mmap()]: %s\n", strerror(errno));
		close(fd);
		return -1;
	}


// check '\n' in the end
	if (((uint8_t *)pmmap_void)[size - 1] != '\n')
	{
		printf("ERROR: invalid format SQL_STAT_FILE, broken end\n");
		munmap(pmmap_void, size);
		close(fd);
		return -1;
	}


	scan_rc = scan(pmmap_void, size, psqltag);


// munmap file
	rc = munmap(pmmap_void, size);
	if (rc == -1)
	{
		printf("ERROR[munmap()]: %s\n", strerror(errno));
		close(fd);
		return -1;
	}


// close file
	rc = close(fd);
	if (rc == -1)
	{
		printf("ERROR[close()]: %s\n", strerror(errno));
		return -1;
	}


	return scan_rc;
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// view help
void help()
{
	printf("%s    %s\n", PROG_FULL_NAME, PROG_URL);
	printf("example: %s [-min|-max|-real] SQL_STAT_FILE [SQL_TAG]\n", PROG_NAME);
	printf("\n");

	printf("aggregate date from SQL_STAT_FILE, sort and show.\n");
	printf("\n");

	printf("  -h, -help, --help    this message\n");
	printf("  -min                 sort by min  execution time (default)\n");
	printf("  -max                 sort by max  execution time\n");
	printf("  -real                sort by real execution time\n");
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
// general function
int main(int argc, char *argv[])
{
	int rc;
	const char *pfilename = NULL;
	const char *psqltag   = NULL;


	if (argc == 1)
	{
		help();
		return 1;
	}


// parse command line args
	for (int i=1; i < argc; i++)
	{
		if ((strcmp(argv[i], "--help") == 0) || (strcmp(argv[i], "-help") == 0) || (strcmp(argv[i], "-h") == 0))
		{
			help();
			return 0;
		}

		if (strcmp(argv[i], "-min") == 0)
		{
			global::sort_type = global::SORT_MIN;
			continue;
		}

		if (strcmp(argv[i], "-max") == 0)
		{
			global::sort_type = global::SORT_MAX;
			continue;
		}

		if (strcmp(argv[i], "-real") == 0)
		{
			global::sort_type = global::SORT_REAL;
			continue;
		}

		if (pfilename == NULL)
		{
			pfilename = argv[i];
			continue;
		}

		psqltag = argv[i];
	}
	if (pfilename == NULL)
	{
		help();
		return 1;
	}


	rc = do_it(pfilename, psqltag);
	if (rc == -1) return 1;
	fflush(stdout);


	return 0;
}
//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
