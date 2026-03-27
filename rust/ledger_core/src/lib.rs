use pyo3::prelude::*;

#[pyfunction]
fn apply_fill_py(position_qty: f64, avg_price: f64, side: &str, qty: f64, price: f64) -> (f64, f64) {
    if side == "BUY" {
        let next_qty = position_qty + qty;
        let next_avg = if next_qty == 0.0 {
            0.0
        } else {
            ((position_qty * avg_price) + (qty * price)) / next_qty
        };
        (next_qty, next_avg)
    } else {
        (position_qty - qty, avg_price)
    }
}

#[pymodule]
fn cta_ledger(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(apply_fill_py, m)?)?;
    Ok(())
}
