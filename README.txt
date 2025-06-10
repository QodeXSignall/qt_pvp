PDS (Port Data Splitter) - это программный разветлитель данных, реализованный в виде TCP сервера,
прослушивающего некоторый порт (USB, COM) и рассылающего эти данные своим клиентам (subscribers).

Пример использования (Рассыльщик данных с USB на порту 2290 на Linux):
pds = PortDataSplitter('0.0.0.0', 2290, port_name='/dev/ttyUSB0', debug=False, device_name='USB listener')