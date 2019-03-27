abstract type DataConsumer end
function consume_header(fn::Function, consumer::DataConsumer, file::File, iter_res)
    new_iter_res = if iter_res !== nothing && file.header_exists
        (line, state) = iter_res
        i = 1
        for field_header in file.fields_buff
            fn(field_header, i)
            i += 1
        end
        iterate(file, state)
    else
        iter_res
    end
	produce_header(consumer, file)
	new_iter_res
end
function consume_rec(pc::DataConsumer, f::File, line, fields)
    if !validate_record(pc.rec_type, length(fields))
        consume_error(pc, f, line)
    else
        for (i, field) in enumerate(fields)
            consume_field(pc, f, field, i)
        end
    end
end
function consume_field() end
function consume_error() end

abstract type RecordLength end
struct UnknownRecordLength <: RecordLength end
struct FixedRecordLength <: RecordLength
    len::Int
end
Base.length(::UnknownRecordLength) = -1
Base.length(rec_len::FixedRecordLength) = rec_len.len

abstract type RecordType end
function record_length() end
function validate_record(rec_type::RecordType, len::UInt)
    rec_len = record_length(rec_type)
    rec_len == FixedRecordLength(len) || rec_len == UnknownRecordLength()
end

struct UntypedRecord <: RecordType
    num_fields::Int
end
function record_length(rec_type::UntypedRecord)
    rec_type.num_fields <= 0 ? UnknownRecordLength() : FixedRecordLength(rec_type.num_fields)
end

const NO_TYPE = 1
const INT_TYPE = 2
const FLOAT_TYPE = 3
const STRING_TYPE = 4
const OTHER_TYPE = 5

const FieldType = Int
const FieldUnionType = Vector{FieldType}

get_field_type(::Type{Int}) = INT_TYPE
get_field_type(::Type{Float64}) = FLOAT_TYPE
get_field_type(::Type{String}) = STRING_TYPE
get_field_type(::Type{T}) where {T} = OTHER_TYPE

function get_field_types(tp::Type{Union{A,B}}) where {A,B}
	if tp isa DataType
		[get_field_type(tp)]
	elseif tp.b isa DataType
		[get_field_type(tp.b), get_field_type(tp.a)]
	else
		push!(get_field_types(tp.b), get_field_type(tp.a))
	end
end

function get_field_union_types(::Type{T}) where {T <: Tuple}
	res = Vector{FieldUnionType}()
	for field_tp in T.parameters
		field_types = get_field_types(field_tp)
		sort!(field_types) # sort types to follow the type precedence
		push!(res, field_types)
	end
	res
end

struct FieldHandler{HANDLER <: Function}
	fn::HANDLER
end

struct TypedRecord{FIELD_HANDLERS <: Tuple} <: RecordType
    field_types::Vector{FieldUnionType}
	other_type_handlers::FIELD_HANDLERS
	fallback_type::FieldType
	function TypedRecord(field_types::Vector{FieldUnionType}=Vector{FieldUnionType}(),
		                 fallback_type::FieldType=NO_TYPE,
						 other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS}
		new{FIELD_HANDLERS}(field_types, other_type_handlers, fallback_type)
	end
end

function TypedRecord(field_types::Type{T},
					 fallback_type::FieldType=NO_TYPE,
					 other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS, T <: Tuple}
	TypedRecord(get_field_union_types(field_types), other_type_handlers, fallback_type)
end

function record_length(rec_type::TypedRecord)
    length(rec_type.field_types) == 0 ? UnknownRecordLength() : FixedRecordLength(rec_type.num_fields)
end

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
function TypedPrintConsumer(out::IO_TP, num_fields::Int=-1) where {IO_TP}
	TypedPrintConsumer{IO_TP}(out, TypedRecord(num_fields), Vector{String}())
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
		csv_field_string(pc.out, f, field_header, i)
	end
	j = i
	for n in (i+1):length(record_length(pc.rec_type))
		j += 1
		csv_field_string(pc.out, f, "$n", n)
	end
	j > 0 && write(pc.out, "\n")
end

function type_str(tp::FieldType)
	    if tp == INT_TYPE;    "int"
	elseif tp == FLOAT_TYPE;  "float"
	elseif tp == STRING_TYPE; "string"
	else;                     "other"
	end
end

type_str(tp::FieldUnionType) = join(tp, ",")

type_str_in_header(tp::FieldUnionType) = " ($(type_str(tp)))"

function type_str_in_header(pc::TypedPrintConsumer, i::Int)
	if i <= length(pc.rec_type)
		type_str_in_header(pc.rec_type[i])
	else
		type_str_in_header(OTHER_TYPE)
	end
end

function produce_header(pc::TypedPrintConsumer, f::File)
	i = 0
	for field_header in pc.headers
		i += 1
		csv_field_string(pc.out, f, "$field_header$(type_str_in_header(pc, i))", i)
	end
	
	for n in (i+1):length(record_length(pc.rec_type))
		csv_field_string(pc.out, f, "$n$(type_str_in_header(pc, n))", n)
	end
	write(pc.out, "\n")
end
function consume_field(pc::UntypedPrintConsumer, f::File, field_str, index::Int)
    csv_field_string(pc.out, f, field_str, index)
    index == length(f.fields_buff) && write(pc.out, "\n")
end
function consume_field(pc::TypedPrintConsumer, f::File, field_str, index::Int)
    
	csv_field_string(pc.out, f, field_str, index)
    index == length(f.fields_buff) && write(pc.out, "\n")
end
function consume_error(pc::PrintConsumer, f::File, line_str)
    write(pc.out, "ERROR >> ")
	write(pc.out, "$(length(f.fields_buff)) fields:")
	for (i, field) in enumerate(f.fields_buff)
		write(pc.out, "\n    $i ->")
		write(pc.out, field)
	end
	write(pc.out, " in \nline: ")
    write(pc.out, string(line_str))
end

function consume(consumer::DataConsumer, f::File)
    iter_res = iterate(f)
    iter_res = consume_header(consumer, f, iter_res)
    while iter_res !== nothing
        (line, state) = iter_res
        consume_rec(consumer, f, line, f.fields_buff)
        iter_res = iterate(f, state)
    end
end

function csv_string(buff::IO, csv_file::File, consumer::DataConsumer = PrintConsumer(buff))
	consume(consumer, csv_file)
	for line in csv_file
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

export DataConsumer, PrintConsumer, consume, consume_header, consume_rec, consume_field, consume_error
export RecordLength, UnknownRecordLength, FixedRecordLength
export RecordType, UntypedRecord, record_length, csv_string
