DECLARE @a date;
set @a = '{0}'
DECLARE @b date;
set @b = '{1}'

EXECUTE [dbo].[хп_ВыгрузкаДляСайта_ПродажиДилеров] 
@НачалоПериода = @a,
@ОкончаниеПериода = @b
