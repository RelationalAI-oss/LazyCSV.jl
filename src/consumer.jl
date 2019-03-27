using Dates

const SHOW_DETAILED_ERROR = false

abstract type DataConsumer end
function consume_header(fn::Function, consumer::DataConsumer, file::File, iter_res)
    new_iter_res = if iter_res !== nothing && file.header_exists
        ((pos, line), state) = iter_res
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
function consume_field() end
function consume_rec_error() end
function consume_field_error() end
function consume_rec(pc::DataConsumer, f::File, pos::FilePos, line, fields)
    if !validate_record(pc.rec_type, length(fields))
        consume_rec_error(pc, f, pos, line)
    else
        for (i, field) in enumerate(fields)
            if !consume_field(pc, f, field, i)
				consume_field_error(pc, f, field, i, pos, line)
			end
        end
    end
end

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

const NO_TYPE       = 0b00000000 % UInt8
const MISSING_TYPE  = 0b10000000 % UInt8
const INT_TYPE      = 0b00000001 % UInt8
const FLOAT_TYPE    = 0b00000010 % UInt8
const DATE_TYPE     = 0b00000100 % UInt8
const DATETIME_TYPE = 0b00001000 % UInt8
const BOOL_TYPE     = 0b00010000 % UInt8
const CHAR_TYPE     = 0b00100000 % UInt8
const STRING_TYPE   = 0b01000000 % UInt8
const OTHER_TYPE    = 0b01111111 % UInt8

const FieldType = UInt8

typecode(::Type{Int64}) = INT_TYPE
typecode(::Type{Float64}) = FLOAT_TYPE
typecode(::Type{Date}) = DATE_TYPE
typecode(::Type{DateTime}) = DATETIME_TYPE
typecode(::Type{Bool}) = BOOL_TYPE
typecode(::Type{String}) = STRING_TYPE
typecode(::Type{Char}) = CHAR_TYPE
typecode(::Type{Tuple{Ptr{UInt8}, Int}}) = STRING_TYPE
typecode(::Type{Missing}) = MISSING_TYPE
typecode(::Type) = OTHER_TYPE

function typecodes(tp::Type{Union{A,B}}) where {A,B}
	if tp isa DataType
		typecode(tp)
	elseif tp.b isa DataType
		typecode(tp.b) | typecode(tp.a)
	else
		typecodes(tp.b) | typecode(tp.a)
	end
end

function get_field_union_types(::Type{T}) where {T <: Tuple}
	res = Vector{FieldType}()
	for field_tp in T.parameters
		field_types = typecodes(field_tp)
		push!(res, field_types)
	end
	res
end

struct FieldHandler{HANDLER <: Function}
	fn::HANDLER
end

struct TypedRecord{FIELD_HANDLERS <: Tuple} <: RecordType
    field_types::Vector{FieldType}
	fallback_type::FieldType
	other_type_handlers::FIELD_HANDLERS
	function TypedRecord(field_types::Vector{FieldType}=Vector{FieldType}(),
		                 fallback_type::FieldType=NO_TYPE,
						 other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS}
		new{FIELD_HANDLERS}(field_types, fallback_type, other_type_handlers)
	end
end

function TypedRecord(field_types::Type{T},
					 fallback_type::FieldType=NO_TYPE,
					 other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS, T <: Tuple}
	TypedRecord(get_field_union_types(field_types), fallback_type, other_type_handlers)
end

function record_length(rec_type::TypedRecord)
    length(rec_type.field_types) == 0 ? UnknownRecordLength() : FixedRecordLength(length(rec_type.field_types))
end


function field_type(rec_type::TypedRecord, i::Int)
	if i <= length(rec_type.field_types)
		rec_type.field_types[i]
	elseif i <= length(rec_type.other_type_handlers)
		OTHER_TYPE
	else
		rec_type.fallback_type
	end
end

ismissing_value(field_value::AbstractString) = isempty(field_value)

function has_valid_type(tp::FieldType, field_value::AbstractString)
	tp == NO_TYPE && return (false, NO_TYPE)
	tp & MISSING_TYPE  != 0 && ismissing_value(field_value) && return (true, MISSING_TYPE)
	tp == OTHER_TYPE && return (true, OTHER_TYPE)
	    if tp & INT_TYPE      != 0 && tryparse(Int, field_value) !== nothing; return (true, INT_TYPE)
	elseif tp & FLOAT_TYPE    != 0 && tryparse(Float64, field_value) !== nothing; return (true, FLOAT_TYPE)
	elseif tp & DATE_TYPE     != 0 && tryparse(Date, field_value) !== nothing; return (true, DATE_TYPE)
	elseif tp & DATETIME_TYPE != 0 && tryparse(DateTime, field_value) !== nothing; return (true, DATETIME_TYPE)
	elseif tp & BOOL_TYPE     != 0 && tryparse(Bool, field_value) !== nothing; return (true, BOOL_TYPE)
	elseif tp & CHAR_TYPE     != 0 && length(field_value) == 1; return (true, CHAR_TYPE)
	elseif tp & STRING_TYPE   != 0; return (true, STRING_TYPE)
	end
	return (false, NO_TYPE)
end

function consume(consumer::DataConsumer, f::File)
    iter_res = iterate(f)
    iter_res = consume_header(consumer, f, iter_res)
    while iter_res !== nothing
        ((pos, line), state) = iter_res
        consume_rec(consumer, f, pos, line, f.fields_buff)
        iter_res = iterate(f, state)
    end
end

include("print_consumer.jl")

export DataConsumer, consume, consume_header, consume_rec, consume_field, consume_rec_error,
       consume_field_error
export RecordLength, UnknownRecordLength, FixedRecordLength
export RecordType, UntypedRecord, record_length, csv_string
