const DEFAULT_DELIM = ','
const DEFAULT_QUOTE = '"'
const DEFAULT_LINE_LEN = 2048
const DEFAULT_NUM_FIELDS = 16
const DEFAULT_EAGER_PARSE_FIELDS = true

mutable struct Counter
    v::Int
end

struct File{IO_TYPE}
    input::IO_TYPE
    delim::UInt8      # the delimiter should be an ASCII character to fit in a single byte
	quotechar::UInt8  # the quote character should be an ASCII character to fit in a single byte
	escapechar::UInt8 # the escape character should be an ASCII character to fit in a single byte
    header_exists::Bool
    eager_parse_fields::Bool
    line_buff::Vector{UInt8}
    fields_buff::BufferedVector{WeakRefString{UInt8}}
    current_line::Counter
    function File(input::IO_TYPE, delim::UInt8, quotechar::UInt8, escapechar::UInt8,
				  header_exists::Bool, eager_parse_fields::Bool, line_buff::Vector{UInt8},
                  fields_buff::BufferedVector{WeakRefString{UInt8}}) where {IO_TYPE}
        new{IO_TYPE}(input, delim, quotechar, escapechar, header_exists,
					 eager_parse_fields, line_buff, fields_buff, Counter(0))
    end
end

function File(input::IO; delim::Char=DEFAULT_DELIM, eager_parse_fields::Bool=DEFAULT_EAGER_PARSE_FIELDS,
			  line_buff_len::Int=DEFAULT_LINE_LEN, fields_buff_len::Int=DEFAULT_NUM_FIELDS,
			  quotechar::Char=DEFAULT_QUOTE, escapechar::Char=quotechar,
			  header_exists::Bool=DEFAULT_HEADER_EXISTS)
    buff = Vector{UInt8}()
    resize!(buff, line_buff_len)

    fields = BufferedVector{WeakRefString{UInt8}}()
    resize!(fields, fields_buff_len)

    File(input, UInt8(delim), UInt8(quotechar), UInt8(escapechar), header_exists,
		eager_parse_fields, buff, fields)
end

function Base.iterate(f::File, state::Int = 1)
    if f.current_line.v + 1 == state
        next_line = read_csv_line!(f)
        if next_line === nothing
            nothing
        else
            f.current_line.v += 1
            (next_line, state+1)
        end
    else
        #TODO: shall we throw an error here?
        # we are expecting the user to always scan sequentially
        nothing
    end
end

result_line_collection(f::File) = Vector{String}()
result_collection(f::File) = Vector{Vector{String}}()

function materialize_line(f::File, line)
    line_vec = result_line_collection(f)
    for field in f.fields_buff
        push!(line_vec, string(field))
    end
    line_vec
end

function materialize(f::File, vec)
    counter = 0
    while (line = read_csv_line!(f)) != nothing
        push!(vec, materialize_line(f, line))
        counter += 1
    end
    f.current_line.v = counter
    nothing
end

function materialize(f::File)
    materialize(f, result_collection(f))
end

function num_fields_for_current_line(f::File)
    length(f.fields_buff)
end

function count_lines(csv_file::File)
	counter = UInt(0)
	for line in csv_file
		counter += 1
	end
	counter
end

function count_fields(csv_file::File)
	counter = UInt(0)
	for line in csv_file
		counter += num_fields_for_current_line(csv_file)
	end
	counter
end

function csv_field_string(buff::IO, csv_file::File, field, i)
	quotechar = Char(csv_file.quotechar)
	escapechar = Char(csv_file.escapechar)
	delim = Char(csv_file.delim)
	has_delim = occursin(delim, field)
	has_quote = occursin(quotechar, field)
	if has_delim || has_quote
		if has_quote
			field_escape_quotes = replace(field, quotechar => "$escapechar$quotechar")
			write(buff, "$quotechar$field_escape_quotes$quotechar")
		else
			write(buff, "$quotechar$field$quotechar")
		end
	else
		write(buff, field)
	end
	if i < length(csv_file.fields_buff)
		write(buff, csv_file.delim)
	end
end

export num_fields_for_current_line, count_lines, count_fields
