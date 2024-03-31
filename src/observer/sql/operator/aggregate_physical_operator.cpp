#include "sql/operator/aggregate_physical_operator.h"
#include "sql/parser/value.h"
#include "storage/field/field.h"

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation) {
  aggregations_.push_back(aggregation);
}

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("aggregate operator must have one child");
    return RC::INTERNAL;
  }

  return children_[0]->open(trx);
}

RC AggregatePhysicalOperator::next()
{
  // already aggregated
  if (result_tuple_.cell_num() > 0) {
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  int count = 0;

  std::vector<Value> result_cells;
  while (RC::SUCCESS == (rc = oper->next())) {
    // get tuple
    Tuple *tuple = oper->current_tuple();

    // do aggregate
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];

      Value cell;
      AttrType attr_type = AttrType::INTS;
      switch (aggregation) {
        case AggrOp::AGGR_SUM:
        case AggrOp::AGGR_AVG:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (!count){
            result_cells.push_back(Value(0.0f));
          }
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;

        case AggrOp::AGGR_MAX:
          rc = tuple->cell_at(cell_idx, cell);
          if (!count){
            result_cells.push_back(cell);
          }else if(cell.compare(result_cells[cell_idx]) > 0){
            result_cells[cell_idx] = cell;
          }
          break;
        
        case AggrOp::AGGR_MIN:
          rc = tuple->cell_at(cell_idx, cell);
          if (!count){
            result_cells.push_back(cell);
          }else if(cell.compare(result_cells[cell_idx]) < 0){
            result_cells[cell_idx] = cell;
          }
          break;

        case AggrOp::AGGR_COUNT:
          if (count == 0) {
            result_cells.push_back(Value(0));
          }
          result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
          break;

        default:
          return RC::UNIMPLEMENT;
      }
    }

    count++;
  }
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  // avg
  for (int cell_idx = 0; cell_idx < (int)result_cells.size(); cell_idx++) {
    const AggrOp aggr = aggregations_[cell_idx];
    if (aggr == AggrOp::AGGR_AVG) {
      result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() / count);
    }
  }
  
  result_tuple_.set_cells(result_cells);

  return rc;
}

RC AggregatePhysicalOperator::close()
{
  return children_[0]->close();
}

Tuple *AggregatePhysicalOperator::current_tuple()
{
  LOG_TRACE("return result tuple");
  return &result_tuple_;
}