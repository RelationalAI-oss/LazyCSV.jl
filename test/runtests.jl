using LazyCSV
using Testy

using CSV
using Mmap
using TextParse

DO_BENCHMARK = false

# creates a IO instance using the given CSV string
function csv_io(csv::AbstractString)
    csv_io = IOBuffer()
    print(csv_io, csv)
    seekstart(csv_io)
    csv_io
end
csv_io(csv::IO) = csv

function csv_count_lines(csv; header_exists=false)
	csv_file = LazyCSV.csvread(csv; header_exists=header_exists, eager_parse_fields=false)
	count_lines(csv_file)
end

function csv_count_fields(csv; delim=',', header_exists=false, quotechar=LazyCSV.DEFAULT_QUOTE, escapechar=quotechar)
	csv_file = LazyCSV.csvread(csv; delim=delim, header_exists=header_exists, eager_parse_fields=true, quotechar=quotechar, escapechar=escapechar)
	count_fields(csv_file)
end

function csv_string(csv; delim=',', header_exists=false, quotechar=LazyCSV.DEFAULT_QUOTE, escapechar=quotechar)
	csv_file = LazyCSV.csvread(csv; delim=delim, header_exists=header_exists, eager_parse_fields=true, quotechar=quotechar, escapechar=escapechar)
	LazyCSV.csv_string(csv_file)
end

function csv_equals(base_csv, to_csv)
	base_csv_io = csv_io(base_csv)
	to_csv_io = csv_io(to_csv)
	for (base_line, to_line) in zip(eachline(base_csv_io), eachline(to_csv_io))
		@test csv_line_equals(base_line, to_line)
	end
end

function strip_csv_line(line)
	line = replace(line, r"^\"" => "")
	line = replace(line, r"\"\$" => "")
	line = replace(line, ",\"" => ",")
	line = replace(line, "\"," => ",")
	line = replace(line, "\"\"" => "\"")
	line
end

function csv_line_equals(base_line, toaa_line)
	if base_line != toaa_line
		strip_csv_line(base_line) == strip_csv_line(toaa_line)
	else
		true
	end
end

@testset "Untyped Tests" begin
	lineitem_sample = """
	1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the
	1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold 
	1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep
	1|2132|4633|4|28|28955.64|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR|lites. fluffily even de
	1|24027|1534|5|24|22824.48|0.10|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB| pending foxes. slyly re
	1|15635|638|6|32|49620.16|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|arefully slyly ex
	2|106170|1191|1|38|44694.46|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a
	3|4297|1798|1|45|54058.05|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco
	3|19036|6540|2|49|46796.47|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve
	3|128449|3474|3|27|39890.88|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|nal foxes wake. 
	"""

	@test csv_count_lines(csv_io(lineitem_sample)) == 10
	@test csv_count_fields(csv_io(lineitem_sample); delim='|') == 160
	csv_equals(lineitem_sample, csv_string(csv_io(lineitem_sample); delim='|'))

	quoted_csv = """
	John,Doe,120 jefferson st.,Riverside, NJ, 08075
	Jack,McGinnis,220 hobo Av.,Phila, PA,09119
	"John ""Da Man""\",Repici,120 Jefferson St.,Riverside, NJ,08075
	Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234
	,Blankman,,SomeTown, SD, 00298
	,Blankman,"",SomeTown, SD, 00298
	"Joan ""\""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123
	"""

	@test csv_count_lines(csv_io(quoted_csv)) == 7
	@test csv_count_fields(csv_io(quoted_csv); delim=',') == 42
	csv_equals(quoted_csv, csv_string(csv_io(quoted_csv); delim=','))

	quoted_csv = """
	John,Doe,120 jefferson st.,Riverside, NJ, 08075
	Jack,McGinnis,220 hobo Av.,Phila, PA,09119
	"John %"Da Man%"",Repici,120 Jefferson St.,Riverside, NJ,08075
	Stephen,Tyler,"7452 Terrace %"At the Plaza%" road",SomeTown,SD, 91234
	,Blankman,,SomeTown, SD, 00298
	,Blankman,"",SomeTown, SD, 00298
	"Joan %"%"the bone%", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123
	"""

	@test csv_count_lines(csv_io(quoted_csv)) == 7
	@test csv_count_fields(csv_io(quoted_csv); delim=',', escapechar='%') == 42
	csv_equals(quoted_csv, csv_string(csv_io(quoted_csv); delim=',', escapechar='%'))
end

function use_csv_jl(filename)
	count = 0
	for row in CSV.File(filename; delim="|")
		count += 1
	end
	count
end

function use_textparse_jl(filename)
	count = 0
	for row in TextParse.csvread(filename, '|'; header_exists=false)
		count += 1
	end
	count
end

function manual_read(filename)
	file = open(filename, "r")
	mmaped_file = Mmap.mmap(file)
	str = String(mmaped_file)
	count = 0
	for x in str
		if x == '\n'
			count += 1
		end
	end
	count
end


function use_lazycsv_jl(filename, eager_parse_fields)
	csv_file = LazyCSV.csvread(filename; delim='|', header_exists=false, eager_parse_fields=eager_parse_fields)
	counter = 0
	for line in csv_file
		counter += 1
	end
	counter
end


if DO_BENCHMARK
	filename = abspath(joinpath(dirname(@__FILE__), "..", "lineitem.tbl"))

	res = 6001215

	println("use_lazycsv_jl (without parsing all fields):")
	v = @time use_lazycsv_jl(filename, false); @show v
	v = @time use_lazycsv_jl(filename, false); @show v
	v = @time use_lazycsv_jl(filename, false); @show v

	println("use_lazycsv_jl (with parsing all fields):")
	v = @time use_lazycsv_jl(filename, true); @show v
	v = @time use_lazycsv_jl(filename, true); @show v
	v = @time use_lazycsv_jl(filename, true); @show v

	println("manual read:")
	v = @time manual_read(filename); @show v
	v = @time manual_read(filename); @show v
	v = @time manual_read(filename); @show v

	println("use_csv_jl:")
	v = @time use_csv_jl(filename); @show v
	v = @time use_csv_jl(filename); @show v
	v = @time use_csv_jl(filename); @show v

	# println("use_textparse_jl:")
	# @show @time use_textparse_jl(filename)
	# @show @time use_textparse_jl(filename)
	# @show @time use_textparse_jl(filename)
end
