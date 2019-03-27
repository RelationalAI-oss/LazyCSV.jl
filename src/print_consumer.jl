struct PrintConsumer{REC_TYPE <:RecordType, IO_TP <: IO} <: DataConsumer
    out::IO_TP
	rec_type::REC_TYPE
    headers::Vector{String}
end

const UntypedPrintConsumer{IO_TP} = PrintConsumer{UntypedRecord, IO_TP}
const TypedPrintConsumer{IO_TP} = PrintConsumer{TypedRecord, IO_TP}
function UntypedPrintConsumer(out::IO_TP, num_fields::Int=-1) where {IO_TP}
	UntypedPrintConsumer{IO_TP}(out, UntypedRecord(num_fields), Vector{String}())
end

function TypedPrintConsumer(out::IO_TP, field_types::Type{T},
							fallback_type::FieldType=NO_TYPE,
							other_type_handlers=Tuple{}()) where {IO_TP, T}
	TypedPrintConsumer{IO_TP}(out, TypedRecord(field_types, fallback_type, other_type_handlers), Vector{String}())
end

PrintConsumer(out::IO_TP, num_fields::Int=-1) where {IO_TP} = UntypedPrintConsumer(out, num_fields)

function consume_header(pc::PrintConsumer, f::File, iter_res)
    consume_header(pc, f, iter_res) do field_header, i
        push!(pc.headers, string(field_header))
    end
end

function produce_header(pc::UntypedPrintConsumer, f::File)
	i = 0
	for field_header in pc.headers
		i += 1
		csv_field_string(pc.out, f, field_header, i, true)
	end
	j = i
	for n in (i+1):length(record_length(pc.rec_type))
		j += 1
		csv_field_string(pc.out, f, "$n", n, true)
	end
	j > 0 && write(pc.out, "\n")
end

function type_str(tp::FieldType)
	types = Vector{String}()
	if tp == OTHER_TYPE; push!(types, "other")
	else
		if tp & INT_TYPE      != 0; push!(types, "int"); end
		if tp & FLOAT_TYPE    != 0; push!(types, "float"); end
		if tp & DATE_TYPE     != 0; push!(types, "date"); end
		if tp & DATETIME_TYPE != 0; push!(types, "datetime"); end
		if tp & BOOL_TYPE     != 0; push!(types, "bool"); end
		if tp & CHAR_TYPE     != 0; push!(types, "char"); end
		if tp & STRING_TYPE   != 0; push!(types, "string"); end
	end
	if tp & MISSING_TYPE  != 0; push!(types, "missing"); end
	join(types, ",")
end

type_str_in_header(tp::FieldType) = " ($(type_str(tp)))"

function type_str_in_header(pc::TypedPrintConsumer, i::Int)
	type_str_in_header(field_type(pc.rec_type, i))
end

function produce_header(pc::TypedPrintConsumer, f::File)
	i = 0
	for field_header in pc.headers
		i += 1
		csv_field_string(pc.out, f, "$(strip(field_header))$(type_str_in_header(pc, i))", i, true)
	end
	
	for n in (i+1):length(record_length(pc.rec_type))
		csv_field_string(pc.out, f, "$n$(type_str_in_header(pc, n))", n, true)
	end
	write(pc.out, "\n")
end
function consume_field(pc::UntypedPrintConsumer, f::File, field_str, index::Int)
    csv_field_string(pc.out, f, strip(field_str), index)
    index == length(f.fields_buff) && write(pc.out, "\n")
	true
end

function consume_field(pc::TypedPrintConsumer, f::File, field_str, index::Int)
	field_tp = field_type(pc.rec_type, index)
	force_quote = field_tp in [[STRING_TYPE], [CHAR_TYPE]]
	field_value = strip(field_str)
	(isvalid, valid_type) = has_valid_type(field_tp, field_value)
	if isvalid
		csv_field_string(pc.out, f, field_value, index, force_quote)
	    index == length(f.fields_buff) && write(pc.out, "\n")
	end
	isvalid
end

function consume_field_error(pc::PrintConsumer, f::File, field_str, index::Int, pos::FilePos, line)
	csv_field_string(pc.out, f, "FIELD_ERR($(strip(field_str)))", index, true)
	index == length(f.fields_buff) && write(pc.out, "\n")
end

function consume_rec_error(pc::PrintConsumer, f::File, pos::FilePos, line_str)
    write(pc.out, "ERROR >> ")
	if SHOW_DETAILED_ERROR
		write(pc.out, "$(length(f.fields_buff)) fields:")
		for (i, field) in enumerate(f.fields_buff)
			write(pc.out, "\n    $i ->")
			write(pc.out, field)
		end
		write(pc.out, " in \nline: ")
	end
    write(pc.out, string(line_str))
end

function csv_string(buff::IO, csv_file::File, consumer::DataConsumer = PrintConsumer(buff))
	consume(consumer, csv_file)
	for pos_line in csv_file
		(pos, line) = pos_line
		i = 1
		for field in csv_file.fields_buff
			csv_field_string(buff, csv_file, field, i)
			i += 1
		end
		write(buff, "\n")
	end
end

function csv_string(csv_file::File, num_fields::Int=-1)
	buff = IOBuffer()
	csv_string(buff, csv_file, PrintConsumer(buff, num_fields))
	seekstart(buff)
	read(buff, String)
end

function typed_csv_string(csv_file::File, field_types::Type{T},
						  fallback_type::FieldType=NO_TYPE,
						  other_type_handlers=Tuple{}()) where {T}
	buff = IOBuffer()
	csv_string(buff, csv_file, TypedPrintConsumer(buff, field_types, fallback_type, other_type_handlers))
	seekstart(buff)
	read(buff, String)
end

export PrintConsumer
