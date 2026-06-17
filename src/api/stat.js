import axios from "axios";

export function getStatList() {
  return axios.get("http://192.168.88.101:8080/api/stat/list");
}