const { Router } = require("express");
const { createAppointment } = require("../controllers/appointment.controller");

const router = Router();

router.post("/", createAppointment);

module.exports = router;
