abstract type DataConsumer end
function consume_header(fn::Function, consumer::DataConsumer, file::File, iter_res)
    if iter_res !== nothing && file.header_exists
        (line, state) = iter_res
        i = 1
        for field_header in file.fields
            fn(field_header, i)
            i += 1
        end
        iterate(file, state)
    else
        iter_res
    end
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
    rec_type.num_fields < 0 ? UnknownRecordLength() : FixedRecordLength(rec_type.num_fields)
end

struct PrintConsumer{IO_TP <: IO} <: DataConsumer
    out::IO_TP
    rec_type::UntypedRecord
    headers::Vector{String}
    
    PrintConsumer(out::IO_TP, num_fields::Int=-1) where {IO_TP} = new{IO_TP}(out, UntypedRecord(num_fields), Vector{String}())
end

function consume_header(pc::PrintConsumer, f::File, iter_res)
    consume_header(pc, f, iter_res) do
        push!(pc.headers, string(field_header))
        csv_field_string(pc.out, f, field_header, i)
    end
end
function consume_field(pc::PrintConsumer, f::File, field_str, index::Int)
    csv_field_string(pc.out, f, field_str, index)
    index == length(f.fields_buff) && write(pc.out, "\n")
end
function consume_error(pc::PrintConsumer, f::File, line_str)
    write(pc.out, "ERROR>> ")
    write(pc.out, line_str)
end

function consume(consumer::DataConsumer, f::File)
    iter_res = iterate(f)
    consume_header(consumer, f, iter_res)
    while iter_res !== nothing
        (line, state) = iter_res
        consume_rec(consumer, f, line, f.fields_buff)
        iter_res = iterate(f, state)
    end
end

const IS_OK = true
const IS_ERROR = !IS_OK
