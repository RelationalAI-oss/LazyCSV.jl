using LazyCSV
using Testy

using CSV
using Mmap
using TextParse

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
	csv_file = LazyCSV.csvread(filename, '|'; header_exists=false, eager_parse_fields=eager_parse_fields)
	counter = 0
	for line in csv_file
		counter += 1
	end
	counter
end

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
