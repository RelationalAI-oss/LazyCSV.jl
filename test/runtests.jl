using LazyCSV
using Testy

DO_BENCHMARK = false
DEBUG_ISSUE = false

# creates a IO instance using the given CSV string
function csv_io(csv::AbstractString)
    csv_io = IOBuffer()
    print(csv_io, csv)
    seekstart(csv_io)
    csv_io
end
csv_io(csv::IO) = csv

function csv_count_lines(csv; header_exists::Bool=false)
	csv_file = LazyCSV.csvread(csv; header_exists=header_exists, eager_parse_fields=false)
	count_lines(csv_file)
end

function csv_count_fields(csv; delim=',', header_exists::Bool=false, quotechar=LazyCSV.DEFAULT_QUOTE, escapechar=quotechar)
	csv_file = LazyCSV.csvread(csv; delim=delim, header_exists=header_exists, 
	                           eager_parse_fields=true, quotechar=quotechar, escapechar=escapechar)
	count_fields(csv_file)
end

function csv_string(csv, num_fields::Int = -1;
	                delim=',', header_exists::Bool=false, quotechar=LazyCSV.DEFAULT_QUOTE, escapechar=quotechar)
	csv_file = LazyCSV.csvread(csv; delim=delim, header_exists=header_exists,
	                           eager_parse_fields=true, quotechar=quotechar, escapechar=escapechar)
	LazyCSV.csv_string(csv_file, num_fields)
end

function typed_csv_string(csv, field_types::Type{T}, fallback_type::LazyCSV.FieldType=LazyCSV.NO_TYPE,
						  other_type_handlers=Tuple{}();
						  delim=',', header_exists::Bool=false,
	                      quotechar=LazyCSV.DEFAULT_QUOTE, escapechar=quotechar) where {T}
	csv_file = LazyCSV.csvread(csv; delim=delim, header_exists=header_exists,
	                           eager_parse_fields=true, quotechar=quotechar, escapechar=escapechar)
	LazyCSV.typed_csv_string(csv_file, field_types, fallback_type, other_type_handlers)
end

function csv_equals(base_csv, to_csv; delim=',')
	base_csv_io = csv_io(base_csv)
	to_csv_io = csv_io(to_csv)
	for (base_line, to_line) in zip(eachline(base_csv_io), eachline(to_csv_io))
		@test csv_line_equals(base_line, to_line; delim=delim) || 
			error("$(strip_csv_line(base_line, delim)) != $(strip_csv_line(to_line, delim))")
	end
end

isspecial_in_regex(delim) = delim == '|'

escape_char(delim) = if isspecial_in_regex(delim)
		"\\$delim"
	else
		"$delim"
	end

function strip_csv_line(line, delim)
	escaped_delim = escape_char(delim)

	line = replace(line, r"^\s*\"" => "")
	line = replace(line, r"^\s*" => "")
	line = replace(line, r"\"\s*$" => "")
	line = replace(line, r"\s*$" => "")
	line = replace(line, Regex("$(escaped_delim)\\s*\\\"") => delim)
	line = replace(line, Regex("\\\"\\s*$(escaped_delim)") => delim)
	line = replace(line, Regex("\\s*$(escaped_delim)\\s*") => delim)
	line = replace(line, "\"\"" => "\"")
	line
end

function csv_line_equals(base_line, to_line; delim=',')
	if base_line != to_line
		strip_csv_line(base_line, delim) == strip_csv_line(to_line, delim)
	else
		true
	end
end

function simple_csv_test(csv_str, num_lines, num_fields; delim=',', quotechar='"',
	                     escapechar=quotechar, header_exists::Bool=false)
	computed_num_lines = csv_count_lines(csv_io(csv_str))
	@test computed_num_lines == num_lines || error("$computed_num_lines != $num_lines in \n----------------\n$csv_str\n----------------")
	computed_num_fields = csv_count_fields(csv_io(csv_str); delim=delim, quotechar=quotechar, escapechar=escapechar)
	@test computed_num_fields == num_fields || error("$computed_num_fields != $num_fields in \n----------------\n$csv_str\n----------------")
	generated_csv = csv_string(csv_io(csv_str);
       delim=delim, quotechar=quotechar, escapechar=escapechar,
	   header_exists=header_exists)
	csv_equals(replace(csv_str, "\r" => "\r\n"), generated_csv; delim=delim)
end

count_errors(output) = count_rec_errors(output) + count_field_errors(output)
count_rec_errors(output) = count(x -> true, eachmatch(r"ERROR", output))
count_field_errors(output) = count(x -> true, eachmatch(r"FIELD_ERR", output))

@testset "LazyCSV tests" begin
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

	simple_csv_test(lineitem_sample, 10, 160; delim='|')

	quoted_csv1 = """
	John,Doe,120 jefferson st.,Riverside, NJ, 08075
	Jack,McGinnis,220 hobo Av.,Phila, PA,09119
	"John ""Da Man""\",Repici,120 Jefferson St.,Riverside, NJ,08075
	Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234
	,Blankman,,SomeTown, SD, 00298
	,Blankman,"",SomeTown, SD, 00298
	"Joan ""\""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123
	"""

	simple_csv_test(quoted_csv1, 7, 42)

	quoted_csv2 = """
	John,Doe,120 jefferson st.,Riverside, NJ, 08075
	Jack,McGinnis,220 hobo Av.,Phila, PA,09119
	"John %"Da Man%"",Repici,120 Jefferson St.,Riverside, NJ,08075
	Stephen,Tyler,"7452 Terrace %"At the Plaza%" road",SomeTown,SD, 91234
	,Blankman,,SomeTown, SD, 00298
	,Blankman,"",SomeTown, SD, 00298
	"Joan %"%"the bone%", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123
	"""

	simple_csv_test(quoted_csv2, 7, 42; escapechar='%')
	
	airtravel_csv = """
	"Month", "1958", "1959", "1960"
	"JAN",  340,  360,  417
	"FEB",  318,  342,  391
	"MAR",  362,  406,  419
	"APR",  348,  396,  461
	"MAY",  363,  420,  472
	"JUN",  435,  472,  535
	"JUL",  491,  548,  622
	"AUG",  505,  559,  606
	"SEP",  404,  463,  508
	"OCT",  359,  407,  461
	"NOV",  310,  362,  390
	"DEC",  337,  405,  432
	"""
	
	simple_csv_test(airtravel_csv, 13, 52; header_exists=true)
	
	biostats_csv = """
	"Name",     "Sex", "Age", "Height (in)", "Weight (lbs)"
	"Alex",       "M",   41,       74,      170
	"Bert",       "M",   42,       68,      166
	"Carl",       "M",   32,       70,      155
	"Dave",       "M",   39,       72,      167
	"Elly",       "F",   30,       66,      124
	"Fran",       "F",   33,       66,      115
	"Gwen",       "F",   26,       64,      121
	"Hank",       "M",   30,       71,      158
	"Ivan",       "M",   53,       72,      175
	"Jake",       "M",   32,       69,      143
	"Kate",       "F",   47,       69,      139
	"Luke",       "M",   34,       72,      163
	"Myra",       "F",   23,       62,       98
	"Neil",       "M",   36,       75,      160
	"Omar",       "M",   38,       70,      145
	"Page",       "F",   31,       67,      135
	"Quin",       "M",   29,       71,      176
	"Ruth",       "F",   28,       65,      131
	"""
	
	simple_csv_test(biostats_csv, 19, 95; header_exists=true)
    
    quoted_csv3 = """
	John,Doe,120 jefferson st.,Riverside, NJ, 08075
	Jack,McGinnis,220 hobo Av.,Phila, PA,09119,error1
	"John ""Da Man""\",Repici,120 Jefferson St.,Riverside, NJ,08075
	Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234
	,Blankman,,SomeTown, SD, 00298
	,Blankman,"",SomeTown, error2
	"Joan ""\""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123
	"""

	quoted_csv3_out = csv_string(csv_io(quoted_csv3), 6)
	@test count_rec_errors(quoted_csv3_out) == 2
	
	simple_csv_test(airtravel_csv, 13, 52; header_exists=true)

end

@testset "Typed Tests" begin
	biostats_csv1 = """
	"Name",     "Sex", "Age", "Height (in)", "Weight (lbs)"
	"Alex",       "M",   41,       74,      170
	"Bert",       "M",   42,       68,      166
	"Carl",       "M",   32,       70,      155
	"""
	biostats_csv1_out = typed_csv_string(csv_io(biostats_csv1), Tuple{String,Char,Int,Int,Int}; header_exists=true)
	@test "\"Name (string)\",\"Sex (char)\",\"Age (int)\",\"Height (in) (int)\",\"Weight (lbs) (int)\"" == first(eachline(csv_io(biostats_csv1_out)))
	@test count_errors(biostats_csv1_out) == 0
	
	biostats_csv2 = """
	"Name",     "Sex", "Age", "Height (in)", "Weight (lbs)"
	"Alex",       "M",   41,       74,      170
	"Bert",       "M",   42.1,     68,      166
	"Carl",       "M",   32,       70,      155
	"""
	biostats_csv2_out = typed_csv_string(csv_io(biostats_csv2), Tuple{String,Char,Int,Int,Int}; header_exists=true)
	@test "\"Name (string)\",\"Sex (char)\",\"Age (int)\",\"Height (in) (int)\",\"Weight (lbs) (int)\"" == first(eachline(csv_io(biostats_csv2_out)))
	@test count_rec_errors(biostats_csv2_out) == 0
	@test count_field_errors(biostats_csv2_out) == 1
	
	biostats_csv3_out = typed_csv_string(csv_io(biostats_csv2), Tuple{String,Char,Union{Int,Float64},Int,Int}; header_exists=true)
	@test "\"Name (string)\",\"Sex (char)\",\"Age (int,float)\",\"Height (in) (int)\",\"Weight (lbs) (int)\"" == first(eachline(csv_io(biostats_csv3_out)))
	@test count_errors(biostats_csv3_out) == 0
	
	biostats_csv4 = """
	"Name",     "Sex", "Age", "Height (in)", "Weight (lbs)"
	"Alex",       "M",   41,       74,      170
	"Bert",          ,   42.1,     68,      166
	"Carl",       "M",   32,       70,      155
	"""
	biostats_csv4_out = typed_csv_string(csv_io(biostats_csv4), Tuple{String,Char,Union{Int,Float64},Int,Int}; header_exists=true)
	@test "\"Name (string)\",\"Sex (char)\",\"Age (int,float)\",\"Height (in) (int)\",\"Weight (lbs) (int)\"" == first(eachline(csv_io(biostats_csv4_out)))
	@test count_rec_errors(biostats_csv4_out) == 0
	@test count_field_errors(biostats_csv4_out) == 1
	
	biostats_csv5_out = typed_csv_string(csv_io(biostats_csv4), Tuple{String,Union{Char,Missing},Union{Int,Float64},Int,Int}; header_exists=true)
	@test "\"Name (string)\",\"Sex (char,missing)\",\"Age (int,float)\",\"Height (in) (int)\",\"Weight (lbs) (int)\"" == first(eachline(csv_io(biostats_csv5_out)))
	@test count_errors(biostats_csv5_out) == 0
end

additional_fields = Dict("taxables.csv" => 4, "deniro.csv" => 3, "oscar_age_male.csv" => 2,
                         "oscar_age_female.csv" => 2, "freshman_lbs.csv" => 2)

files_without_header = Dict{String, Int}()

@testset "Untyped File Tests" begin
	csv_dir = abspath(joinpath(dirname(@__FILE__), "sample_csv"))
	for csv_file in readdir(csv_dir)
		endswith(csv_file, ".csv") || continue
		DEBUG_ISSUE && println("$csv_file:")
		csv_file_path = joinpath(csv_dir, csv_file)
		csv_file_io = open(csv_file_path, "r")
		csv_file_str = read(csv_file_io, String)
		line_count = count(x -> x == '\n' || x == '\r', csv_file_str)
		delim_count = count(x -> x == ',', csv_file_str)
		field_count = delim_count + line_count
		simple_csv_test(csv_file_str, line_count, field_count-get(additional_fields,
		                csv_file, 0); header_exists=!haskey(files_without_header, csv_file))
	end
end

end
