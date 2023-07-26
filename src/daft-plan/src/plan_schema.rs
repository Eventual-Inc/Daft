use daft_core::datatypes::DataType;

// DataType and Field ID.
// The Field of a LogicalPlan is distinct from the Field of a Table because it needs a field ID concept,
// which has no meaning at the data representation level.
pub struct PlanField {
    pub datatype: DataType,
    pub field_id: String,
}

// Column name -> field information.
pub type PlanSchema = indexmap::IndexMap<String, PlanField>;
